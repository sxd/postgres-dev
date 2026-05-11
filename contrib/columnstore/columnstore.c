/*-------------------------------------------------------------------------
 *
 * columnstore.c
 *	  Columnar storage table access method.
 *
 * This file contains the extension entry point (_PG_init), the table AM
 * handler function, and the static TableAmRoutine definition that wires
 * up all columnstore callbacks.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/columnstore.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/heapam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/reloptions.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/pg_am.h"
#include "catalog/storage.h"
#include "catalog/storage_xlog.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "storage/read_stream.h"
#include "storage/smgr.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

/* Forward declarations */
static const TupleTableSlotOps *cs_slot_callbacks(Relation rel);
static TM_Result cs_delta_delete(Relation rel, ItemPointer tid,
								 CommandId cid, TransactionId xid,
								 bool wait, TM_FailureData *tmfd);
static TM_Result cs_tuple_delete(Relation rel, ItemPointer tid,
								 CommandId cid, uint32 options,
								 Snapshot snapshot, Snapshot crosscheck,
								 bool wait, TM_FailureData *tmfd);
static TM_Result cs_tuple_update(Relation rel, ItemPointer otid,
								 TupleTableSlot *slot, CommandId cid,
								 uint32 options, Snapshot snapshot,
								 Snapshot crosscheck, bool wait,
								 TM_FailureData *tmfd,
								 LockTupleMode *lockmode,
								 TU_UpdateIndexes *update_indexes);
static TM_Result cs_tuple_lock(Relation rel, ItemPointer tid,
							   Snapshot snapshot, TupleTableSlot *slot,
							   CommandId cid, LockTupleMode mode,
							   LockWaitPolicy wait_policy, uint8 flags,
							   TM_FailureData *tmfd);
static bool cs_fetch_row_version(Relation rel, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot);
static bool cs_tuple_tid_valid(TableScanDesc scan, ItemPointer tid);
static void cs_get_latest_tid(TableScanDesc scan, ItemPointer tid);
static bool cs_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
										Snapshot snapshot);
static TransactionId cs_index_delete_tuples(Relation rel,
											TM_IndexDeleteOp *delstate);
static IndexFetchTableData *cs_index_fetch_begin(Relation rel, uint32 flags);
static void cs_index_fetch_reset(IndexFetchTableData *data);
static void cs_index_fetch_end(IndexFetchTableData *data);
static bool cs_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot,
								 bool *call_again, bool *all_dead);
static void cs_relation_set_new_filelocator(Relation rel,
											const RelFileLocator *newrlocator,
											char persistence,
											TransactionId *freezeXid,
											MultiXactId *minmulti);
static void cs_relation_nontransactional_truncate(Relation rel);
static void cs_relation_copy_data(Relation rel,
								  const RelFileLocator *newrlocator);
static void cs_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
										 Relation OldIndex, bool use_sort,
										 TransactionId OldestXmin,
										 Snapshot snapshot,
										 TransactionId *xid_cutoff,
										 MultiXactId *multi_cutoff,
										 double *num_tuples,
										 double *tups_vacuumed,
										 double *tups_recently_dead);
static void cs_fetch_columnar_row(Relation rel, BlockNumber rg_catalog_block,
								  uint32 row_offset, TupleDesc tupdesc,
								  Datum *values, bool *isnull);
static bool cs_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream);
static bool cs_scan_analyze_next_tuple(TableScanDesc scan,
									   double *liverows, double *deadrows,
									   TupleTableSlot *slot);
static double cs_index_build_range_scan(Relation table_rel,
										Relation index_rel,
										IndexInfo *index_info,
										bool allow_sync,
										bool anyvisible,
										bool progress,
										BlockNumber start_blockno,
										BlockNumber numblocks,
										IndexBuildCallback callback,
										void *callback_state,
										TableScanDesc scan);
static void cs_index_validate_scan(Relation table_rel, Relation index_rel,
								   IndexInfo *index_info,
								   Snapshot snapshot,
								   ValidateIndexState *state);
static uint64 cs_relation_size(Relation rel, ForkNumber forkNumber);
static bool cs_relation_needs_toast_table(Relation rel);
static Oid	cs_relation_toast_am(Relation rel);
static void cs_relation_estimate_size(Relation rel, int32 *attr_widths,
									  BlockNumber *pages, double *tuples,
									  double *allvisfrac);
static bool cs_scan_bitmap_next_tuple(TableScanDesc scan,
									  TupleTableSlot *slot, bool *recheck,
									  uint64 *lossy_pages,
									  uint64 *exact_pages);
static bool cs_scan_sample_next_block(TableScanDesc scan,
									  SampleScanState *scanstate);
static bool cs_scan_sample_next_tuple(TableScanDesc scan,
									  SampleScanState *scanstate,
									  TupleTableSlot *slot);
static Size cs_parallelscan_estimate(Relation rel);
static Size cs_parallelscan_initialize(Relation rel,
									   ParallelTableScanDesc pscan);
static void cs_parallelscan_reinitialize(Relation rel,
										 ParallelTableScanDesc pscan);

/* ----------------------------------------------------------------
 * Slot callbacks
 * ----------------------------------------------------------------
 */
static const TupleTableSlotOps *
cs_slot_callbacks(Relation rel)
{
	return &TTSOpsColumnStore;
}

/* ----------------------------------------------------------------
 * Tuple deletion and update
 * ----------------------------------------------------------------
 */

/*
 * Delete a delta-store tuple by marking its xmax.
 *
 * Checks for concurrent modification and acquires a heavyweight tuple lock
 * for conflict detection.  If 'wait' is true, blocks on a conflicting
 * in-progress transaction; otherwise returns TM_WouldBlock.
 */
static TM_Result
cs_delta_delete(Relation rel, ItemPointer tid, CommandId cid,
				TransactionId xid, bool wait, TM_FailureData *tmfd)
{
	Buffer		buf;
	Page		page;
	ItemId		itemid;
	HeapTupleHeader htup;
	GenericXLogState *state;
	TransactionId xmax;
	CommandId	cmax;
	bool		iscombo;

	for (;;)
	{
		buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
		LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);

		page = BufferGetPage(buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			return TM_Invisible;
		}

		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		xmax = HeapTupleHeaderGetRawXmax(htup);

		if (!TransactionIdIsValid(xmax) ||
			(htup->t_infomask & HEAP_XMAX_INVALID))
		{
			/* No deleter -- row is live, proceed */
			break;
		}

		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			tmfd->cmax = HeapTupleHeaderGetCmax(htup);
			tmfd->xmax = xmax;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			return TM_SelfModified;
		}

		/*
		 * Check in-progress before committed: xact.c records the commit in
		 * pg_xact before clearing the proc array entry, so the reverse order
		 * could misread a just-committed deleter as aborted.
		 */
		if (TransactionIdIsInProgress(xmax))
		{
			/*
			 * Wait on the deleting transaction's xid (not the tuple lock,
			 * which the deleter does not hold while idle in its transaction),
			 * so we sleep instead of spinning and the deadlock detector sees
			 * the wait edge.
			 */
			UnlockReleaseBuffer(buf);

			if (!wait)
				return TM_WouldBlock;

			XactLockTableWait(xmax, rel, tid, XLTW_Delete);
			CHECK_FOR_INTERRUPTS();
			continue;			/* re-check after the deleter finishes */
		}

		if (TransactionIdDidCommit(xmax))
		{
			/*
			 * Columnstore rows have no update chains (UPDATE is delete +
			 * insert at an unrelated TID), so a committed deleter means the
			 * row is gone, not superseded: report TM_Deleted, as heap does
			 * when ctid would equal the tuple's own TID.  TM_Updated here
			 * would send the executor chasing a "next version" that does not
			 * exist.
			 */
			tmfd->xmax = xmax;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			return TM_Deleted;
		}

		/*
		 * The deleter aborted (or crashed): the row is live.  Proceed; the
		 * new xmax simply overwrites the aborted one.
		 */
		break;
	}

	/*
	 * Acquire heavyweight lock before modifying.  Release buffer lock first
	 * to avoid lock ordering issues, then re-read and re-check.
	 */
	UnlockReleaseBuffer(buf);

	if (wait)
		LockTuple(rel, tid, AccessExclusiveLock);
	else
	{
		if (!ConditionalLockTuple(rel, tid, AccessExclusiveLock, false))
			return TM_WouldBlock;
	}

	/* Re-read and re-check under heavyweight lock */
	buf = ReadBuffer(rel, ItemPointerGetBlockNumber(tid));
	LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
	page = BufferGetPage(buf);

	/* see above */
	if (!CSPageIsDelta(page))
	{
		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);
		return TM_Invisible;
	}

	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));

	if (!ItemIdIsNormal(itemid))
	{
		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);
		return TM_Invisible;
	}

	/*
	 * If a running compaction has fenced this page, it has already collected
	 * the page's rows and will consume it: an xmax stamped now would be
	 * silently discarded.  Wait for the compaction to either consume the page
	 * (the row has moved to columnar under a new TID we cannot chase; report
	 * it deleted) or discard its batch (retry).
	 */
	if (CSDeltaPageIsFenced(page))
	{
		CSMetaPageData fmeta;
		BlockNumber blkno = ItemPointerGetBlockNumber(tid);

		UnlockReleaseBuffer(buf);
		UnlockTuple(rel, tid, AccessExclusiveLock);

		if (!wait)
			return TM_WouldBlock;

		LockPage(rel, CS_METAPAGE_BLKNO, ShareLock);
		UnlockPage(rel, CS_METAPAGE_BLKNO, ShareLock);
		CHECK_FOR_INTERRUPTS();

		cs_read_metapage(rel, &fmeta);
		if (fmeta.cs_delta_nblocks == 0 ||
			blkno < fmeta.cs_delta_start ||
			blkno >= fmeta.cs_delta_start + fmeta.cs_delta_nblocks)
		{
			tmfd->xmax = InvalidTransactionId;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			return TM_Deleted;
		}
		return cs_delta_delete(rel, tid, cid, xid, wait, tmfd);
	}

	htup = (HeapTupleHeader) PageGetItem(page, itemid);
	xmax = HeapTupleHeaderGetRawXmax(htup);

	if (TransactionIdIsValid(xmax) &&
		!(htup->t_infomask & HEAP_XMAX_INVALID))
	{
		/*
		 * Another deleter slipped in between our check and taking the tuple
		 * lock.  Same classification as above; an aborted xmax falls through
		 * and is overwritten.
		 */
		if (TransactionIdIsCurrentTransactionId(xmax))
		{
			tmfd->cmax = HeapTupleHeaderGetCmax(htup);
			tmfd->xmax = xmax;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);
			return TM_SelfModified;
		}
		else if (TransactionIdIsInProgress(xmax))
		{
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);

			if (!wait)
				return TM_WouldBlock;

			XactLockTableWait(xmax, rel, tid, XLTW_Delete);
			CHECK_FOR_INTERRUPTS();
			return cs_delta_delete(rel, tid, cid, xid, wait, tmfd);
		}
		else if (TransactionIdDidCommit(xmax))
		{
			/* no update chains; see the first committed-xmax site */
			tmfd->xmax = xmax;
			tmfd->cmax = InvalidCommandId;
			ItemPointerCopy(tid, &tmfd->ctid);
			UnlockReleaseBuffer(buf);
			UnlockTuple(rel, tid, AccessExclusiveLock);
			return TM_Deleted;
		}
	}

	/*
	 * If this transaction also inserted the row, cmin and cmax share the
	 * single t_cid field and a combo command id must be used, or an open
	 * cursor positioned between the insert and this delete would misread its
	 * cmin and skip the row.  Computed before the WAL operation starts
	 * because it can allocate (and so fail).
	 */
	cmax = cid;
	HeapTupleHeaderAdjustCmax(htup, &cmax, &iscombo);

	/* Set xmax via GenericXLog */
	state = GenericXLogStart(rel);
	page = GenericXLogRegisterBuffer(state, buf, 0);
	itemid = PageGetItemId(page, ItemPointerGetOffsetNumber(tid));
	htup = (HeapTupleHeader) PageGetItem(page, itemid);

	htup->t_infomask &= ~(HEAP_XMAX_COMMITTED | HEAP_XMAX_INVALID |
						  HEAP_XMAX_IS_MULTI | HEAP_XMAX_LOCK_ONLY |
						  HEAP_XMAX_KEYSHR_LOCK | HEAP_XMAX_EXCL_LOCK);
	HeapTupleHeaderSetXmax(htup, xid);
	HeapTupleHeaderSetCmax(htup, cmax, iscombo);

	GenericXLogFinish(state);
	UnlockReleaseBuffer(buf);

	/* Release the heavyweight tuple lock */
	UnlockTuple(rel, tid, AccessExclusiveLock);

	return TM_Ok;
}

static TM_Result
cs_tuple_delete(Relation rel, ItemPointer tid, CommandId cid,
				uint32 options, Snapshot snapshot, Snapshot crosscheck,
				bool wait, TM_FailureData *tmfd)
{
	if (ItemPointerGetBlockNumber(tid) >= CS_COLUMNAR_BLKNO_BASE)
		elog(ERROR, "columnstore: columnar DELETE not yet supported");

	return cs_delta_delete(rel, tid, cid, GetCurrentTransactionId(),
						   wait, tmfd);
}

static TM_Result
cs_tuple_update(Relation rel, ItemPointer otid, TupleTableSlot *slot,
				CommandId cid, uint32 options, Snapshot snapshot,
				Snapshot crosscheck, bool wait, TM_FailureData *tmfd,
				LockTupleMode *lockmode, TU_UpdateIndexes *update_indexes)
{
	TM_Result	result;

	/* DELETE the old row */
	result = cs_tuple_delete(rel, otid, cid, options, snapshot, crosscheck,
							 wait, tmfd);
	if (result != TM_Ok)
		return result;

	/* INSERT the new row into the delta store */
	cs_tuple_insert(rel, slot, cid, 0, NULL);

	*update_indexes = TU_All;
	return TM_Ok;
}

static TM_Result
cs_tuple_lock(Relation rel, ItemPointer tid, Snapshot snapshot,
			  TupleTableSlot *slot, CommandId cid, LockTupleMode mode,
			  LockWaitPolicy wait_policy, uint8 flags,
			  TM_FailureData *tmfd)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		/*
		 * Columnar rows are immutable — updates are implemented as delete +
		 * insert into delta.  To "lock" a columnar row, just verify it hasn't
		 * been deleted and fetch it into the slot.
		 */
		if (!cs_fetch_row_version(rel, tid, snapshot, slot))
			return TM_Deleted;

		return TM_Ok;
	}
	else
	{
		/*
		 * Delta store row: use heavyweight tuple locking analogous to the
		 * delete path.  We re-fetch the tuple under lock.
		 */
		if (!cs_fetch_row_version(rel, tid, snapshot, slot))
			return TM_Deleted;

		return TM_Ok;
	}
}

/* ----------------------------------------------------------------
 * Tuple visibility callbacks
 * ----------------------------------------------------------------
 */

/*
 * Fetch all columns of a single columnar row through a private column cache.
 *
 * Reads the row group catalog and decompresses each column to extract the
 * value at row_offset.  Used by single-row index fetch / EvalPlanQual paths
 * where we don't have a scan descriptor with a persistent cache.
 */
static void
cs_fetch_columnar_row(Relation rel, BlockNumber rg_catalog_block,
					  uint32 row_offset, TupleDesc tupdesc,
					  Datum *values, bool *isnull)
{
	int			natts = tupdesc->natts;
	CSColumnCache cache;
	char		ni64_buf[CS_NI64_BUF_SIZE];

	memset(&cache, 0, sizeof(cache));

	if (!cs_load_rowgroup_into(&cache, rel, rg_catalog_block, natts))
	{
		/* Should not happen if caller verified the row group exists */
		for (int col = 0; col < natts; col++)
		{
			values[col] = (Datum) 0;
			isnull[col] = true;
		}
		return;
	}

	for (int col = 0; col < natts; col++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, col);

		cs_ensure_column_loaded(&cache, rel, col);

		/* Extract value from the loaded column */
		if (cache.col_dict[col])
		{
			CSDictColumn *dict = cache.col_dict[col];
			uint32		idx;

			switch (dict->index_width)
			{
				case 1:
					idx = ((uint8 *) dict->index_data)[row_offset];
					break;
				case 2:
					idx = cs_read_u16((const char *) dict->index_data + (Size) (row_offset) * sizeof(uint16));
					break;
				default:
					idx = cs_read_u32((const char *) dict->index_data + (Size) (row_offset) * sizeof(uint32));
					break;
			}

			if (dict->has_null && idx == dict->dict_count)
			{
				isnull[col] = true;
				values[col] = (Datum) 0;
			}
			else
			{
				isnull[col] = false;
				values[col] = datumCopy(dict->dict_values[idx],
										attr->attbyval, attr->attlen);
			}
		}
		else if (cache.col_values[col] != NULL)
		{
			/* varlen path: values point into raw_data buffer */
			values[col] = datumCopy(cache.col_values[col][row_offset],
									attr->attbyval, attr->attlen);
			isnull[col] = cache.col_nulls[col][row_offset];
		}
		else
		{
			/* Fixed-len by-val: direct access from raw_data */
			if (cache.col_null_bitmap[col] &&
				CS_ISNULL(cache.col_null_bitmap[col], row_offset))
			{
				isnull[col] = true;
				values[col] = (Datum) 0;
			}
			else if (cache.col_ni64_dscale[col] > 0)
			{
				int64		ival = ((int64 *) cache.col_raw_data[col])[row_offset];

				isnull[col] = false;
				values[col] = datumCopy(
										cs_int64_to_numeric_buf(ival,
																(int) cache.col_ni64_dscale[col],
																ni64_buf),
										false, -1);
			}
			else
			{
				isnull[col] = false;
				values[col] = cs_fetch_att(cache.col_raw_data[col] +
										   (Size) row_offset * attr->attlen,
										   true, attr->attlen);
			}
		}
	}

	cs_column_cache_free(&cache, natts);
}

static bool
cs_fetch_row_version(Relation rel, ItemPointer tid,
					 Snapshot snapshot, TupleTableSlot *slot)
{
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	ExecClearTuple(slot);

	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		/* Columnar row: decode virtual TID and read column data */
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		uint32		rg_id;
		uint32		row_offset;
		CSMetaPageData meta;
		CSRowGroupDesc *rg_desc;
		Size		rg_size;
		int			natts;
		BlockNumber rg_catalog_block;

		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) / CS_VIRTUAL_BLOCKS_PER_RG;
		row_offset = ((blkno - CS_COLUMNAR_BLKNO_BASE) %
					  CS_VIRTUAL_BLOCKS_PER_RG)
			* CS_ROWS_PER_VIRTUAL_BLOCK + (offnum - 1);

		cs_read_metapage(rel, &meta);

		if (rg_id >= meta.cs_nrowgroups)
			return false;

		/* Read directory to find catalog block for this row group */
		{
			BlockNumber *rgdir = cs_read_rgdir(rel, &meta);

			if (rgdir == NULL)
				return false;
			rg_catalog_block = rgdir[rg_id];
			pfree(rgdir);
		}
		if (rg_catalog_block == InvalidBlockNumber)
			return false;

		natts = tupdesc->natts;
		rg_size = CSRowGroupDescSize(natts);
		rg_desc = palloc(rg_size);
		cs_read_rowgroup_catalog(rel, rg_catalog_block, rg_desc, natts);

		if (row_offset >= rg_desc->rg_num_rows)
		{
			pfree(rg_desc);
			return false;
		}

		/* Check deletion bitmap */
		if (rg_desc->rg_delbitmap_block != InvalidBlockNumber)
		{
			if (cs_delbitmap_test_bit(rel, rg_desc->rg_delbitmap_block,
									  row_offset))
			{
				pfree(rg_desc);
				return false;
			}
		}

		pfree(rg_desc);

		/* Fetch all columns for this row via the column cache */
		cs_fetch_columnar_row(rel, rg_catalog_block, row_offset,
							  tupdesc, slot->tts_values, slot->tts_isnull);

		ExecStoreVirtualTuple(slot);
		ItemPointerCopy(tid, &slot->tts_tid);
		slot->tts_tableOid = RelationGetRelid(rel);

		return true;
	}
	else
	{
		/* Delta store: read the page and check visibility */
		Buffer		buf;
		Page		page;
		ItemId		itemid;
		OffsetNumber offnum = ItemPointerGetOffsetNumber(tid);
		HeapTupleData tuple;

		/*
		 * Reject TIDs that cannot reference a delta page at all: the
		 * metapage, anything past EOF (a fabricated WHERE ctid = ... probe).
		 * TIDs below the current delta range are deliberately allowed
		 * through: an index entry may reference a page a compaction has
		 * consumed, whose contents remain intact and readable until the page
		 * is reused -- and reuse requires AccessExclusiveLock, taken only
		 * after indexes are rebuilt (see CS_META_PENDING_REINDEX).  The
		 * page-identity check below screens out everything that is not (or no
		 * longer) a delta page.
		 */
		if (blkno == CS_METAPAGE_BLKNO ||
			blkno >= RelationGetNumberOfBlocks(rel))
			return false;

		buf = ReadBuffer(rel, blkno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
		page = BufferGetPage(buf);

		/* a foreign page interleaved into the range is not delta content */
		if (!CSPageIsDelta(page) ||
			offnum < FirstOffsetNumber ||
			offnum > PageGetMaxOffsetNumber(page))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		itemid = PageGetItemId(page, offnum);
		if (!ItemIdIsNormal(itemid))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
		tuple.t_len = ItemIdGetLength(itemid);
		tuple.t_tableOid = RelationGetRelid(rel);
		ItemPointerCopy(tid, &tuple.t_self);

		/* Skip tombstone tuples — they are not data rows */
		if (CS_IS_TOMBSTONE(tuple.t_data))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		if (snapshot != SnapshotAny &&
			!cs_delta_satisfies_visibility(&tuple, snapshot))
		{
			UnlockReleaseBuffer(buf);
			return false;
		}

		heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
						  slot->tts_values, slot->tts_isnull);
		ExecStoreVirtualTuple(slot);
		slot->tts_tid = tuple.t_self;
		slot->tts_tableOid = tuple.t_tableOid;

		UnlockReleaseBuffer(buf);
		return true;
	}
}

static bool
cs_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	CScanDesc	cscan = (CScanDesc) scan;
	BlockNumber blkno = ItemPointerGetBlockNumber(tid);

	/* Columnar TIDs must point at an existing row group */
	if (blkno >= CS_COLUMNAR_BLKNO_BASE)
	{
		uint32		rg_id = (blkno - CS_COLUMNAR_BLKNO_BASE) /
			CS_VIRTUAL_BLOCKS_PER_RG;

		return rg_id < cscan->nrowgroups;
	}

	/*
	 * Delta TIDs must point into the delta page range; anything else
	 * (metapage, row-group catalog, column data) is not a row.
	 */
	return cscan->delta_nblocks > 0 &&
		blkno >= cscan->delta_start &&
		blkno < cscan->delta_start + cscan->delta_nblocks;
}

static void
cs_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	/* no-op: columnstore doesn't do HOT chains */
}

static bool
cs_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
							Snapshot snapshot)
{
	TupleTableSlot *testslot;
	bool		visible;

	/*
	 * Re-fetch the row at the slot's TID under the caller's snapshot.  The
	 * fetch path applies the delta-store MVCC test or, for columnar rows, the
	 * deletion-bitmap and pending-tombstone tests, which is exactly the
	 * verdict callers (ON CONFLICT DO UPDATE, EvalPlanQual) need. Always
	 * returning "visible" here would let an ON CONFLICT update modify a row
	 * its snapshot cannot see, instead of raising the serialization failure
	 * REPEATABLE READ requires.
	 */
	testslot = table_slot_create(rel, NULL);
	visible = cs_fetch_row_version(rel, &slot->tts_tid, snapshot, testslot);
	ExecDropSingleTupleTableSlot(testslot);

	return visible;
}

static TransactionId
cs_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	elog(ERROR, "columnstore: index_delete_tuples not yet supported");
}

/* ----------------------------------------------------------------
 * Index fetch callbacks
 * ----------------------------------------------------------------
 */
static IndexFetchTableData *
cs_index_fetch_begin(Relation rel, uint32 flags)
{
	elog(ERROR, "columnstore: index scans not yet supported");
}

static void
cs_index_fetch_reset(IndexFetchTableData *data)
{
	elog(ERROR, "columnstore: index scans not yet supported");
}

static void
cs_index_fetch_end(IndexFetchTableData *data)
{
	elog(ERROR, "columnstore: index scans not yet supported");
}

static bool
cs_index_fetch_tuple(IndexFetchTableData *scan, ItemPointer tid,
					 Snapshot snapshot, TupleTableSlot *slot,
					 bool *call_again, bool *all_dead)
{
	elog(ERROR, "columnstore: index scans not yet supported");
}

/* ----------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------
 */
static void
cs_relation_set_new_filelocator(Relation rel,
								const RelFileLocator *newrlocator,
								char persistence,
								TransactionId *freezeXid,
								MultiXactId *minmulti)
{
	SMgrRelation srel;

	*freezeXid = RecentXmin;
	*minmulti = GetOldestMultiXactId();

	srel = RelationCreateStorage(*newrlocator, persistence, true);

	if (persistence == RELPERSISTENCE_UNLOGGED)
	{
		smgrcreate(srel, INIT_FORKNUM, false);
		log_smgrcreate(newrlocator, INIT_FORKNUM);
	}

	smgrclose(srel);

	/*
	 * Initialize the metapage.  We need to open the relation via its new
	 * filelocator to do this.  Since the relation cache entry still points to
	 * the old locator, we do this in cs_storage.c via a separate path that
	 * uses the smgr directly.
	 */
}

static void
cs_relation_nontransactional_truncate(Relation rel)
{
	RelationTruncate(rel, 0);
}

static void
cs_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	SMgrRelation dstrel;

	FlushRelationBuffers(rel);

	dstrel = RelationCreateStorage(*newrlocator,
								   rel->rd_rel->relpersistence, true);

	RelationCopyStorage(RelationGetSmgr(rel), dstrel, MAIN_FORKNUM,
						rel->rd_rel->relpersistence);

	for (ForkNumber forkNum = MAIN_FORKNUM + 1;
		 forkNum <= MAX_FORKNUM; forkNum++)
	{
		if (smgrexists(RelationGetSmgr(rel), forkNum))
		{
			smgrcreate(dstrel, forkNum, false);
			if (RelationIsPermanent(rel) ||
				(rel->rd_rel->relpersistence == RELPERSISTENCE_UNLOGGED &&
				 forkNum == INIT_FORKNUM))
				log_smgrcreate(newrlocator, forkNum);
			RelationCopyStorage(RelationGetSmgr(rel), dstrel, forkNum,
								rel->rd_rel->relpersistence);
		}
	}

	RelationDropStorage(rel);
	smgrclose(dstrel);
}

static void
cs_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
							 Relation OldIndex, bool use_sort,
							 TransactionId OldestXmin,
							 Snapshot snapshot,
							 TransactionId *xid_cutoff,
							 MultiXactId *multi_cutoff,
							 double *num_tuples,
							 double *tups_vacuumed,
							 double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("CLUSTER is not supported for columnstore tables")));
}

/* ----------------------------------------------------------------
 * ANALYZE callbacks
 * ----------------------------------------------------------------
 */
static bool
cs_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	CScanDesc	cscan = (CScanDesc) scan;

	/*
	 * Phase 1: consume blocks from the read stream, but only accept delta
	 * pages.  The stream samples all relation pages, so we skip the metapage,
	 * row group catalog pages, and column data pages.
	 */
	if (!cscan->delta_done)
	{
		for (;;)
		{
			BlockNumber blkno;

			cscan->delta_buf = read_stream_next_buffer(stream, NULL);
			if (!BufferIsValid(cscan->delta_buf))
			{
				cscan->delta_done = true;
				break;
			}

			blkno = BufferGetBlockNumber(cscan->delta_buf);

			/* Only process delta store pages */
			if (blkno >= cscan->delta_start &&
				blkno < cscan->delta_start + cscan->delta_nblocks)
			{
				LockBuffer(cscan->delta_buf, BUFFER_LOCK_SHARE);
				cscan->delta_cur_offset = FirstOffsetNumber;
				return true;
			}

			/* Not a delta page — release and try next */
			ReleaseBuffer(cscan->delta_buf);
			cscan->delta_buf = InvalidBuffer;
		}
	}

	/*
	 * Phase 2: iterate through columnar row groups.  Load the next unscanned
	 * row group so analyze_next_tuple can yield its rows.
	 */
	if (!cscan->columnar_done && cscan->cur_rowgroup < cscan->nrowgroups)
	{
		cs_load_rowgroup(cscan, scan->rs_rd, cscan->cur_rowgroup);
		cscan->cur_row_in_group = 0;
		return true;
	}

	cscan->columnar_done = true;
	return false;
}

static bool
cs_scan_analyze_next_tuple(TableScanDesc scan, double *liverows,
						   double *deadrows, TupleTableSlot *slot)
{
	CScanDesc	cscan = (CScanDesc) scan;

	/*
	 * If we have a delta buffer, yield tuples from it.
	 */
	if (BufferIsValid(cscan->delta_buf))
	{
		Page		page = BufferGetPage(cscan->delta_buf);
		OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

		while (cscan->delta_cur_offset <= maxoff)
		{
			ItemId		itemid;
			HeapTupleData tuple;

			itemid = PageGetItemId(page, cscan->delta_cur_offset);
			cscan->delta_cur_offset++;

			if (!ItemIdIsNormal(itemid))
			{
				if (ItemIdIsDead(itemid))
					*deadrows += 1;
				continue;
			}

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(scan->rs_rd);
			ItemPointerSet(&tuple.t_self,
						   BufferGetBlockNumber(cscan->delta_buf),
						   cscan->delta_cur_offset - 1);

			/* Skip tombstone tuples — they are not data rows */
			if (CS_IS_TOMBSTONE(tuple.t_data))
				continue;

			/*
			 * Only rows visible to ANALYZE's snapshot may enter the sample;
			 * aborted, uncommitted, and deleted rows otherwise pollute
			 * pg_statistic and reltuples.
			 */
			if (!cs_delta_satisfies_visibility(&tuple, GetActiveSnapshot()))
			{
				*deadrows += 1;
				continue;
			}

			*liverows += 1;

			ExecClearTuple(slot);
			heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
							  slot->tts_values, slot->tts_isnull);
			ExecStoreVirtualTuple(slot);
			return true;
		}

		/* Done with this delta block */
		UnlockReleaseBuffer(cscan->delta_buf);
		cscan->delta_buf = InvalidBuffer;
		ExecClearTuple(slot);
		return false;
	}

	/*
	 * Yield rows from the current columnar row group.
	 */
	if (cscan->colcache.col_nrows > 0)
	{
		while (cscan->cur_row_in_group < cscan->colcache.col_nrows)
		{
			uint32		row = cscan->cur_row_in_group++;

			/* Skip deleted rows */
			if (cscan->colcache.cur_delbitmap &&
				CS_ISDELETED(cscan->colcache.cur_delbitmap, row))
			{
				*deadrows += 1;
				continue;
			}

			*liverows += 1;

			ExecClearTuple(slot);

			/* Set up lazy loading via CSTupleTableSlot */
			{
				CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
				BlockNumber vblk;
				OffsetNumber voff;

				csslot->cs_scan = cscan;
				csslot->cs_colcache = &cscan->colcache;
				csslot->cs_row = row;
				csslot->cs_is_columnar = true;

				slot->tts_flags &= ~TTS_FLAG_EMPTY;
				slot->tts_nvalid = 0;

				vblk = CS_COLUMNAR_BLKNO_BASE +
					cscan->cur_rowgroup * CS_VIRTUAL_BLOCKS_PER_RG +
					row / CS_ROWS_PER_VIRTUAL_BLOCK;
				voff = (row % CS_ROWS_PER_VIRTUAL_BLOCK) + 1;
				ItemPointerSet(&slot->tts_tid, vblk, voff);
			}
			slot->tts_tableOid = RelationGetRelid(scan->rs_rd);
			return true;
		}

		/* Done with this row group, advance to next */
		cscan->cur_rowgroup++;
		return false;
	}

	return false;
}

/* ----------------------------------------------------------------
 * Index build callback
 * ----------------------------------------------------------------
 */
static double
cs_index_build_range_scan(Relation table_rel, Relation index_rel,
						  IndexInfo *index_info, bool allow_sync,
						  bool anyvisible, bool progress,
						  BlockNumber start_blockno, BlockNumber numblocks,
						  IndexBuildCallback callback,
						  void *callback_state, TableScanDesc scan)
{
	elog(ERROR, "columnstore: index builds not yet supported");
	return 0;
}

static void
cs_index_validate_scan(Relation table_rel, Relation index_rel,
					   IndexInfo *index_info, Snapshot snapshot,
					   ValidateIndexState *state)
{
	elog(ERROR, "columnstore: index validation not yet supported");
}

/* ----------------------------------------------------------------
 * Size / estimation callbacks
 * ----------------------------------------------------------------
 */
static uint64
cs_relation_size(Relation rel, ForkNumber forkNumber)
{
	return table_block_relation_size(rel, forkNumber);
}

/*
 * Decide whether a columnstore relation needs an associated TOAST table.
 *
 * Incoming inserts are buffered as ordinary 8 KB heap tuples in the delta
 * store, so the heap rule applies: if the maximum tuple length could
 * exceed TOAST_TUPLE_THRESHOLD, a TOAST table is required so
 * heap_toast_insert_or_update can push oversized varlena attributes out
 * of line.  The compactor later detoasts those attributes when it
 * serializes them into the column chunks.
 *
 * The logic mirrors heapam_relation_needs_toast_table().
 */
static bool
cs_relation_needs_toast_table(Relation rel)
{
	int32		data_length = 0;
	bool		maxlength_unknown = false;
	bool		has_toastable_attrs = false;
	TupleDesc	tupdesc = rel->rd_att;
	int32		tuple_length;
	int			i;

	for (i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);

		if (att->attisdropped)
			continue;
		if (att->attgenerated == ATTRIBUTE_GENERATED_VIRTUAL)
			continue;
		data_length = att_align_nominal(data_length, att->attalign);
		if (att->attlen > 0)
		{
			/* Fixed-length types are never toastable */
			data_length += att->attlen;
		}
		else
		{
			int32		maxlen = type_maximum_size(att->atttypid,
												   att->atttypmod);

			if (maxlen < 0)
				maxlength_unknown = true;
			else
				data_length += maxlen;
			if (att->attstorage != TYPSTORAGE_PLAIN)
				has_toastable_attrs = true;
		}
	}
	if (!has_toastable_attrs)
		return false;
	if (maxlength_unknown)
		return true;
	tuple_length = MAXALIGN(SizeofHeapTupleHeader +
							BITMAPLEN(tupdesc->natts)) +
		MAXALIGN(data_length);
	return (tuple_length > TOAST_TUPLE_THRESHOLD);
}

/*
 * TOAST tables for columnstore relations are plain heap tables; the
 * columnstore AM does not host TOAST chunks itself.
 */
static Oid
cs_relation_toast_am(Relation rel)
{
	return HEAP_TABLE_AM_OID;
}

/*
 * Compute scan-cost scaling factors for a columnstore relation.
 *
 * Of the six outputs, only *cpu_tuple_cost_factor is currently consumed
 * (by cs_set_rel_pathlist_hook, to scale the per-tuple portion of the
 * inherited baseline path's cost).  The remaining outputs --
 * seq_page_cost_factor, rand_page_cost_factor, rand_io_pages,
 * disk_cost_parallelizable, index_fetch_cost -- are computed for a
 * future cost-modifier hook in costsize.c that would let the AM scale
 * page costs and supply a per-row-group random-I/O unit; today nothing
 * reads them, so index-scan and bitmap-scan costing on columnstore
 * relations falls back to the heap defaults.
 */
void
cs_relation_cost_factors(Relation rel,
						 double *seq_page_cost_factor,
						 double *rand_page_cost_factor,
						 double *cpu_tuple_cost_factor,
						 BlockNumber *rand_io_pages,
						 bool *disk_cost_parallelizable,
						 double *index_fetch_cost)
{
	/*
	 * Columnar scans decompress and project columns, which is cheaper
	 * per-tuple than heap's row-oriented processing when only a subset of
	 * columns is needed.  Use a lower CPU tuple cost factor.
	 */
	*seq_page_cost_factor = 1.0;
	*rand_page_cost_factor = 1.0;
	*cpu_tuple_cost_factor = 0.5;
	*disk_cost_parallelizable = false;
	*index_fetch_cost = 0.0;
	*rand_io_pages = 0;
}

int
cs_compute_parallel_workers(Relation rel)
{
	/* Parallel workers arrive in a later step */
	return 0;
}

static void
cs_relation_estimate_size(Relation rel, int32 *attr_widths,
						  BlockNumber *pages, double *tuples,
						  double *allvisfrac)
{
	BlockNumber nblocks;
	CSMetaPageData meta;
	BlockNumber delta_nblocks;

	nblocks = RelationGetNumberOfBlocks(rel);

	if (nblocks == 0)
	{
		*pages = 0;
		*tuples = 0;
		*allvisfrac = 0;
		return;
	}

	/*
	 * Read the metapage to get cached stats.  cs_data_pages counts only
	 * column data and delta pages (set by VACUUM), giving the planner an
	 * accurate I/O cost without the metadata overhead that
	 * RelationGetNumberOfBlocks includes.  Fall back to the physical block
	 * count when cs_data_pages hasn't been set yet (pre-VACUUM).
	 */
	cs_read_metapage(rel, &meta);

	delta_nblocks = meta.cs_delta_nblocks;
	*pages = (meta.cs_data_pages > 0) ? meta.cs_data_pages : nblocks;

	/*
	 * Estimate tuples from delta-store pages only.  Columnar row group counts
	 * arrive in a later step; for the delta-only AM, the planner sees only
	 * what's in delta pages.
	 */
	*tuples = (double) delta_nblocks * MaxHeapTuplesPerPage;

	*allvisfrac = 0.0;
}

/* ----------------------------------------------------------------
 * Bitmap scan callback
 * ----------------------------------------------------------------
 */
static bool
cs_scan_bitmap_next_tuple(TableScanDesc scan, TupleTableSlot *slot,
						  bool *recheck, uint64 *lossy_pages,
						  uint64 *exact_pages)
{
	elog(ERROR, "columnstore: bitmap scans not yet supported");
	return false;
}

/* ----------------------------------------------------------------
 * TABLESAMPLE callbacks
 * ----------------------------------------------------------------
 */
static bool
cs_scan_sample_next_block(TableScanDesc scan, SampleScanState *scanstate)
{
	elog(ERROR, "columnstore: TABLESAMPLE not yet supported");
	return false;
}

static bool
cs_scan_sample_next_tuple(TableScanDesc scan, SampleScanState *scanstate,
						  TupleTableSlot *slot)
{
	elog(ERROR, "columnstore: TABLESAMPLE not yet supported");
	return false;
}

/* ----------------------------------------------------------------
 * Parallel scan support
 * ----------------------------------------------------------------
 */
static Size
cs_parallelscan_estimate(Relation rel)
{
	elog(ERROR, "columnstore: parallel scans not yet supported");
	return 0;
}

static Size
cs_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnstore: parallel scans not yet supported");
	return 0;
}

static void
cs_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	elog(ERROR, "columnstore: parallel scans not yet supported");
}


/* ----------------------------------------------------------------
 * TableAmRoutine definition
 * ----------------------------------------------------------------
 */

static const TableAmRoutine columnstore_methods = {
	.type = T_TableAmRoutine,

	.amoptions = cs_reloptions,

	.slot_callbacks = cs_slot_callbacks,

	.scan_begin = cs_scan_begin,
	.scan_end = cs_scan_end,
	.scan_rescan = cs_scan_rescan,
	.scan_getnextslot = cs_scan_getnextslot,

	.scan_set_tidrange = cs_scan_set_tidrange,
	.scan_getnextslot_tidrange = cs_scan_getnextslot_tidrange,

	.parallelscan_estimate = cs_parallelscan_estimate,
	.parallelscan_initialize = cs_parallelscan_initialize,
	.parallelscan_reinitialize = cs_parallelscan_reinitialize,

	.index_fetch_begin = cs_index_fetch_begin,
	.index_fetch_reset = cs_index_fetch_reset,
	.index_fetch_end = cs_index_fetch_end,
	.index_fetch_tuple = cs_index_fetch_tuple,

	.tuple_insert = cs_tuple_insert,
	.tuple_insert_speculative = cs_tuple_insert_speculative,
	.tuple_complete_speculative = cs_tuple_complete_speculative,
	.multi_insert = cs_multi_insert,
	.tuple_delete = cs_tuple_delete,
	.tuple_update = cs_tuple_update,
	.tuple_lock = cs_tuple_lock,

	.tuple_fetch_row_version = cs_fetch_row_version,
	.tuple_get_latest_tid = cs_get_latest_tid,
	.tuple_tid_valid = cs_tuple_tid_valid,
	.tuple_satisfies_snapshot = cs_tuple_satisfies_snapshot,
	.index_delete_tuples = cs_index_delete_tuples,

	.relation_set_new_filelocator = cs_relation_set_new_filelocator,
	.relation_nontransactional_truncate = cs_relation_nontransactional_truncate,
	.relation_copy_data = cs_relation_copy_data,
	.relation_copy_for_cluster = cs_relation_copy_for_cluster,
	.relation_vacuum = cs_vacuum_rel,
	.scan_analyze_next_block = cs_scan_analyze_next_block,
	.scan_analyze_next_tuple = cs_scan_analyze_next_tuple,
	.index_build_range_scan = cs_index_build_range_scan,
	.index_validate_scan = cs_index_validate_scan,

	.relation_size = cs_relation_size,
	.relation_needs_toast_table = cs_relation_needs_toast_table,
	.relation_toast_am = cs_relation_toast_am,

	.relation_estimate_size = cs_relation_estimate_size,

	.scan_bitmap_next_tuple = cs_scan_bitmap_next_tuple,
	.scan_sample_next_block = cs_scan_sample_next_block,
	.scan_sample_next_tuple = cs_scan_sample_next_tuple,
};


const TableAmRoutine *
GetColumnstoreAmRoutine(void)
{
	return &columnstore_methods;
}

PG_FUNCTION_INFO_V1(columnstore_tableam_handler);

Datum
columnstore_tableam_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnstore_methods);
}

/*
 * Read the sort_key reloption from a columnstore relation.  The value
 * lives in our own CSRdOptions struct (registered by cs_reloptions);
 * NULL means no hint is set.
 */
const char *
cs_get_sort_key(Relation rel)
{
	return CSRelationGetSortKey(rel);
}

void
_PG_init(void)
{
	DefineCustomRealVariable("columnstore.rowgroup_compaction_threshold",
							 "Fraction of deleted rows that triggers row group compaction during VACUUM.",
							 "Row groups with a dead-row ratio at or above this value are rewritten "
							 "without the deleted rows.  Set to -1 to disable row group compaction.",
							 &columnstore_rowgroup_compaction_threshold,
							 -1.0,
							 -1.0, 1.0,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("columnstore");

	cs_register_reloptions();
}
