/*-------------------------------------------------------------------------
 *
 * cs_storage.c
 *	  Metapage initialization and page format helpers for columnstore.
 *
 * The metapage data is stored in the "special space" area of page 0
 * (via PageInit with a non-zero specialSize).  This ensures the data
 * survives GenericXLog's zeroing of the free-space hole between
 * pd_lower and pd_upper.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_storage.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/generic_xlog.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"

/*
 * Get metapage data pointer from a page initialized with special space.
 */
static inline CSMetaPageData *
cs_metapage_get_data(Page page)
{
	return (CSMetaPageData *) PageGetSpecialPointer(page);
}

/*
 * Initialize the metapage for a newly created columnstore relation.
 *
 * Called from the first INSERT when we detect the relation has no blocks.
 * We extend the relation and initialize the metapage and first delta page.
 */
void
cs_init_metapage(Relation rel)
{
	Buffer		metabuf;
	Buffer		deltabuf;
	Page		metapage;
	Page		deltapage;
	GenericXLogState *state;
	CSMetaPageData *meta;

	/*
	 * Use the relation extension lock to avoid races with concurrent
	 * inserters.  Extend to get 2 blocks: metapage (0) and first delta (1).
	 */
	LockRelationForExtension(rel, ExclusiveLock);

	/* Re-check under lock */
	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		UnlockRelationForExtension(rel, ExclusiveLock);
		return;
	}

	metabuf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK,
								 NULL);
	deltabuf = ReadBufferExtended(rel, MAIN_FORKNUM, P_NEW, RBM_ZERO_AND_LOCK,
								  NULL);

	UnlockRelationForExtension(rel, ExclusiveLock);

	state = GenericXLogStart(rel);

	/*
	 * Initialize metapage with special space for CSMetaPageData. This puts
	 * the metadata in the pd_special..end area which is preserved by
	 * GenericXLog's page copy logic.
	 */
	metapage = GenericXLogRegisterBuffer(state, metabuf,
										 GENERIC_XLOG_FULL_IMAGE);
	PageInit(metapage, BLCKSZ, sizeof(CSMetaPageData));

	meta = cs_metapage_get_data(metapage);
	meta->cs_magic = CS_MAGIC;
	meta->cs_version = CS_VERSION;
	meta->cs_natts = RelationGetDescr(rel)->natts;
	meta->cs_nrowgroups = 0;
	meta->cs_delta_start = BufferGetBlockNumber(deltabuf);
	meta->cs_delta_nblocks = 1;
	meta->cs_total_rows = 0;
	meta->cs_rgdir_start = InvalidBlockNumber;
	meta->cs_rgdir_npages = 0;
	meta->cs_freelist_start = InvalidBlockNumber;
	meta->cs_freelist_npages = 0;
	meta->cs_freelist_nranges = 0;
	meta->cs_data_pages = 0;
	meta->cs_flags = 0;

	/* Initialize first delta page */
	deltapage = GenericXLogRegisterBuffer(state, deltabuf,
										  GENERIC_XLOG_FULL_IMAGE);
	cs_delta_page_init(deltapage);

	GenericXLogFinish(state);

	UnlockReleaseBuffer(deltabuf);
	UnlockReleaseBuffer(metabuf);
}

/*
 * Initialize a delta store page, stamping the identifying opaque.
 */
void
cs_delta_page_init(Page page)
{
	CSDeltaPageOpaque *opaque;

	PageInit(page, BLCKSZ, sizeof(CSDeltaPageOpaque));
	opaque = CSDeltaPageGetOpaque(page);
	opaque->cs_flags = 0;
	opaque->cs_page_id = CS_DELTA_PAGE_ID;
}

/*
 * Read the metapage data.  Caller should not hold any buffer lock.
 */
void
cs_read_metapage(Relation rel, CSMetaPageData *meta)
{
	Buffer		buf;
	Page		page;

	if (RelationGetNumberOfBlocks(rel) == 0)
	{
		/* Not yet initialized -- return empty state */
		memset(meta, 0, sizeof(CSMetaPageData));
		meta->cs_magic = CS_MAGIC;
		meta->cs_version = CS_VERSION;
		meta->cs_delta_start = InvalidBlockNumber;
		meta->cs_rgdir_start = InvalidBlockNumber;
		meta->cs_freelist_start = InvalidBlockNumber;
		return;
	}

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_SHARE);
	page = BufferGetPage(buf);

	memcpy(meta, cs_metapage_get_data(page), sizeof(CSMetaPageData));

	if (meta->cs_magic != CS_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnstore metapage has wrong magic number")));

	if (meta->cs_version != CS_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("columnstore metapage version %u is not supported",
						meta->cs_version),
				 errhint("This table was created with an older columnstore format. "
						 "Dump and reload the table to upgrade.")));

	UnlockReleaseBuffer(buf);
}

/*
 * Atomically merge an insert-side update into the on-disk metapage.
 *
 * new_blkno is the delta page this backend just created by extending the
 * relation, or InvalidBlockNumber if the tuple(s) went into an existing
 * page.  rows_added is the live-row count adjustment (0 for tombstones).
 *
 * All adjustments are applied to the CURRENT on-disk values under one
 * exclusive buffer lock.  The insert paths must never write back a
 * privately held copy of the whole metapage: between their read and
 * write another inserter may have extended the delta or VACUUM may have
 * advanced it, and a full-struct overwrite would silently revert that.
 *
 * The delta range is widened to cover new_blkno.  Pages between the old
 * range end and new_blkno may belong to a concurrent VACUUM's column
 * data; including them is fine because readers identify delta pages by
 * their special-area opaque and skip foreign pages.
 *
 * Returns true if the delta went from empty to non-empty (callers use
 * this to send a relcache invalidation; see cs_delta_do_insert).
 */
bool
cs_metapage_merge_insert(Relation rel, BlockNumber new_blkno, int64 rows_added)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;
	bool		became_nonempty = false;

	if (new_blkno == InvalidBlockNumber && rows_added == 0)
		return false;			/* nothing to record */

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	if (new_blkno != InvalidBlockNumber)
	{
		if (on_disk->cs_delta_nblocks == 0 ||
			on_disk->cs_delta_start == InvalidBlockNumber)
		{
			on_disk->cs_delta_start = new_blkno;
			on_disk->cs_delta_nblocks = 1;
			became_nonempty = true;
		}
		else
		{
			/*
			 * New pages come from extending the relation, so they always lie
			 * at or beyond the current range end; VACUUM only ever moves the
			 * range start forward over already-consumed pages.
			 */
			Assert(new_blkno >= on_disk->cs_delta_start);
			if (new_blkno - on_disk->cs_delta_start + 1 >
				on_disk->cs_delta_nblocks)
				on_disk->cs_delta_nblocks =
					new_blkno - on_disk->cs_delta_start + 1;
		}
	}

	on_disk->cs_total_rows += rows_added;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	return became_nonempty;
}

/*
 * Update only the planner statistics fields of the metapage.
 *
 * Used by VACUUM after compaction: a full cs_write_metapage here would
 * race concurrent inserts (clobbering a delta extension that happened
 * after VACUUM re-read the metapage), so touch nothing but the stats.
 * The row count is itself an estimate; a concurrent insert's increment
 * lost to this overwrite is corrected by the next VACUUM.
 */
void
cs_metapage_update_stats(Relation rel, uint64 total_rows,
						 BlockNumber data_pages)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	on_disk->cs_total_rows = total_rows;
	on_disk->cs_data_pages = data_pages;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Atomically clear metapage flag bits, leaving everything else as it
 * is on disk (so concurrent insert merges are not clobbered).
 */
void
cs_metapage_clear_flags(Relation rel, uint32 flags)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;
	CSMetaPageData *on_disk;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	on_disk = cs_metapage_get_data(page);

	on_disk->cs_flags &= ~flags;

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}

/*
 * Write updated metapage data.
 */
void
cs_write_metapage(Relation rel, CSMetaPageData *meta)
{
	Buffer		buf;
	Page		page;
	GenericXLogState *state;

	buf = ReadBuffer(rel, CS_METAPAGE_BLKNO);
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);

	memcpy(cs_metapage_get_data(page), meta, sizeof(CSMetaPageData));

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);
}
