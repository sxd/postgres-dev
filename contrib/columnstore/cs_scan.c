/*-------------------------------------------------------------------------
 *
 * cs_scan.c
 *	  Sequential scan implementation for columnstore tables.
 *
 * In this commit the AM stores all data in the delta heap, so a scan is
 * a heap-format walk with MVCC visibility checking.  Columnar row group
 * scanning is added by a later commit.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"

static bool cs_delta_getnext(CScanDesc scan, ScanDirection direction,
							 TupleTableSlot *slot);


/*
 * Begin a sequential scan over a columnstore relation.
 */
TableScanDesc
cs_scan_begin(Relation rel, Snapshot snapshot, int nkeys, ScanKeyData *key,
			  ParallelTableScanDesc pscan, uint32 flags)
{
	CScanDesc	scan;

	scan = palloc0(sizeof(CScanDescData));

	scan->rs_base.rs_rd = rel;
	scan->rs_base.rs_snapshot = snapshot;
	scan->rs_base.rs_nkeys = nkeys;
	scan->rs_base.rs_key = key;
	scan->rs_base.rs_flags = flags;
	scan->rs_base.rs_parallel = pscan;

	/* Initialize delta scan state */
	scan->delta_buf = InvalidBuffer;
	scan->delta_done = false;
	scan->delta_cur_block = InvalidBlockNumber;
	scan->delta_cur_offset = InvalidOffsetNumber;

	/*
	 * Allocated here rather than lazily in the scan loop: getnext can run
	 * inside a caller's per-row memory context (e.g. ATRewriteTable), which
	 * must not own scan-lifetime state.
	 */
	scan->delta_visible_offsets =
		palloc(sizeof(OffsetNumber) * MaxHeapTuplesPerPage);
	scan->delta_nvisible = 0;
	scan->delta_visible_idx = 0;

	/* Columnar scan state -- always done at this stage. */
	scan->columnar_done = true;
	scan->cur_rowgroup = 0;
	scan->cur_row_in_group = 0;
	scan->rg_catalog_blocks = NULL;

	if (RelationGetNumberOfBlocks(rel) > 0)
	{
		CSMetaPageData meta;

		cs_read_metapage(rel, &meta);
		scan->delta_start = meta.cs_delta_start;
		scan->delta_nblocks = meta.cs_delta_nblocks;
		scan->nrowgroups = meta.cs_nrowgroups;
		scan->natts = meta.cs_natts;
		scan->delta_cur_block = meta.cs_delta_start;
	}
	else
	{
		scan->delta_done = true;
	}

	scan->needed_col_list = NULL;
	scan->needed_col_count = 0;

	return (TableScanDesc) scan;
}

/*
 * End a scan, release all resources.
 */
void
cs_scan_end(TableScanDesc sscan)
{
	CScanDesc	scan = (CScanDesc) sscan;

	if (sscan->rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(sscan->rs_snapshot);

	if (BufferIsValid(scan->delta_buf))
	{
		ReleaseBuffer(scan->delta_buf);
		scan->delta_buf = InvalidBuffer;
	}

	if (scan->delta_visible_offsets)
		pfree(scan->delta_visible_offsets);

	if (scan->rg_catalog_blocks)
		pfree(scan->rg_catalog_blocks);
	if (scan->needed_cols)
		bms_free(scan->needed_cols);
	if (scan->needed_col_list)
		pfree(scan->needed_col_list);

	pfree(scan);
}

/*
 * Rescan: reset to the beginning.
 */
void
cs_scan_rescan(TableScanDesc sscan, ScanKeyData *key,
			   bool set_params, bool allow_strat,
			   bool allow_sync, bool allow_pagemode)
{
	CScanDesc	scan = (CScanDesc) sscan;

	if (BufferIsValid(scan->delta_buf))
	{
		ReleaseBuffer(scan->delta_buf);
		scan->delta_buf = InvalidBuffer;
	}

	scan->delta_done = (scan->delta_nblocks == 0);
	scan->delta_cur_block = scan->delta_start;
	scan->delta_cur_offset = InvalidOffsetNumber;
	scan->delta_nvisible = 0;
	scan->delta_visible_idx = 0;

	scan->columnar_done = true;
	scan->cur_rowgroup = 0;
	scan->cur_row_in_group = 0;

	if (key)
		sscan->rs_key = key;
}

/*
 * Store the set of columns needed by the query.
 *
 * Only these columns will be decompressed during columnar scan,
 * avoiding I/O and CPU for unused columns.
 */
void
cs_scan_set_projection(TableScanDesc sscan, Bitmapset *needed_cols)
{
	CScanDesc	scan = (CScanDesc) sscan;
	int			col;
	int			count;

	if (scan->needed_cols)
		bms_free(scan->needed_cols);
	if (scan->needed_col_list)
	{
		pfree(scan->needed_col_list);
		scan->needed_col_list = NULL;
	}
	scan->needed_col_count = 0;

	if (needed_cols == NULL || bms_is_empty(needed_cols))
	{
		scan->needed_cols = NULL;
		scan->count_optimized = true;
		return;
	}

	scan->needed_cols = bms_copy(needed_cols);

	count = bms_num_members(needed_cols);
	scan->needed_col_list = palloc(sizeof(int) * count);
	scan->needed_col_count = 0;

	col = -1;
	while ((col = bms_next_member(needed_cols, col)) >= 0)
		scan->needed_col_list[scan->needed_col_count++] = col;
}

/*
 * Visibility test for a delta-store tuple that never dirties the page.
 *
 * Pass NoHintBitsBuffer so the visibility code skips hint-bit maintenance:
 * the delta pages are managed via GenericXLog, and a hint bit written
 * outside a GenericXLogStart/Finish pair would invalidate the page deltas
 * it computes.
 */
bool
cs_delta_satisfies_visibility(HeapTupleData *tuple, Snapshot snapshot)
{
	return HeapTupleSatisfiesVisibility(tuple, snapshot, NoHintBitsBuffer);
}

/*
 * Try to get the next visible tuple from the delta store.
 *
 * Uses page-mode scanning: pre-scans all visible tuple offsets on a page
 * under a single share lock, then yields them one at a time with just the
 * buffer pin held.
 */
static bool
cs_delta_getnext(CScanDesc scan, ScanDirection direction, TupleTableSlot *slot)
{
	Relation	rel = scan->rs_base.rs_rd;

	/*
	 * Only forward scans are implemented.  Erroring beats the silent
	 * wrong-rows alternative of treating a backward request as forward; plans
	 * over the CustomScan provider never ask (it does not set
	 * CUSTOMPATH_SUPPORT_BACKWARD_SCAN), so this guards stray callers.
	 */
	Snapshot	snapshot = scan->rs_base.rs_snapshot;

	if (!ScanDirectionIsForward(direction))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore does not support backward scans")));

	while (!scan->delta_done)
	{
		/* Return next visible tuple from pre-scanned page */
		while (scan->delta_visible_idx < scan->delta_nvisible)
		{
			CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
			Page		page;
			ItemId		itemid;
			HeapTupleData tuple;
			OffsetNumber off;

			off = scan->delta_visible_offsets[scan->delta_visible_idx++];
			page = BufferGetPage(scan->delta_buf);
			itemid = PageGetItemId(page, off);

			tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
			tuple.t_len = ItemIdGetLength(itemid);
			tuple.t_tableOid = RelationGetRelid(rel);
			ItemPointerSet(&tuple.t_self, scan->delta_cur_block, off);

			/*
			 * Deform with the SLOT's descriptor, not the relation's: during
			 * an ALTER TABLE rewrite the relcache already shows the new
			 * column types while these stored tuples (and the rewrite scan's
			 * slot) still have the old format.
			 */
			ExecClearTuple(slot);
			heap_deform_tuple(&tuple, slot->tts_tupleDescriptor,
							  slot->tts_values, slot->tts_isnull);

			csslot->cs_scan = NULL;
			csslot->cs_colcache = NULL;
			csslot->cs_is_columnar = false;
			slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
			slot->tts_flags &= ~TTS_FLAG_EMPTY;
			slot->tts_tid = tuple.t_self;
			slot->tts_tableOid = tuple.t_tableOid;
			return true;
		}

		if (BufferIsValid(scan->delta_buf))
		{
			ReleaseBuffer(scan->delta_buf);
			scan->delta_buf = InvalidBuffer;
			scan->delta_cur_block++;
		}

		if (scan->delta_cur_block >= scan->delta_start + scan->delta_nblocks)
		{
			scan->delta_done = true;
			break;
		}

		scan->delta_buf = ReadBuffer(rel, scan->delta_cur_block);
		LockBuffer(scan->delta_buf, BUFFER_LOCK_SHARE);

		{
			Page		page = BufferGetPage(scan->delta_buf);
			OffsetNumber maxoff = PageGetMaxOffsetNumber(page);

			scan->delta_nvisible = 0;
			scan->delta_visible_idx = 0;

			Assert(scan->delta_visible_offsets != NULL);

			for (OffsetNumber off = FirstOffsetNumber; off <= maxoff; off++)
			{
				ItemId		itemid;
				HeapTupleData tuple;

				itemid = PageGetItemId(page, off);
				if (!ItemIdIsNormal(itemid))
					continue;

				tuple.t_data = (HeapTupleHeader) PageGetItem(page, itemid);
				tuple.t_len = ItemIdGetLength(itemid);
				tuple.t_tableOid = RelationGetRelid(rel);
				ItemPointerSet(&tuple.t_self, scan->delta_cur_block, off);

				if (CS_IS_TOMBSTONE(tuple.t_data))
					continue;

				if (!cs_delta_satisfies_visibility(&tuple, snapshot))
					continue;

				scan->delta_visible_offsets[scan->delta_nvisible++] = off;
			}
		}

		LockBuffer(scan->delta_buf, BUFFER_LOCK_UNLOCK);
	}

	return false;
}

/*
 * Get the next tuple from the scan.  Columnar row groups are scanned by a
 * later commit; at this stage only the delta store is consulted.
 */
bool
cs_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
					TupleTableSlot *slot)
{
	CScanDesc	scan = (CScanDesc) sscan;

	CHECK_FOR_INTERRUPTS();

	if (sscan->rs_parallel != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnstore: parallel scans not yet supported")));

	if (!scan->delta_done)
	{
		if (cs_delta_getnext(scan, direction, slot))
			return true;
	}

	return false;
}

/*
 * cs_scan_set_tidrange -- restrict a scan to a TID range
 */
void
cs_scan_set_tidrange(TableScanDesc sscan, ItemPointer mintid,
					 ItemPointer maxtid)
{
	ItemPointerCopy(mintid, &sscan->st.tidrange.rs_mintid);
	ItemPointerCopy(maxtid, &sscan->st.tidrange.rs_maxtid);
}

/*
 * cs_scan_getnextslot_tidrange -- get next tuple within a TID range
 *
 * Wraps the normal sequential scan, filtering tuples to only return those
 * whose TID falls within [rs_mintid, rs_maxtid].
 */
bool
cs_scan_getnextslot_tidrange(TableScanDesc sscan, ScanDirection direction,
							 TupleTableSlot *slot)
{
	ItemPointer mintid = &sscan->st.tidrange.rs_mintid;
	ItemPointer maxtid = &sscan->st.tidrange.rs_maxtid;

	for (;;)
	{
		if (!cs_scan_getnextslot(sscan, direction, slot))
			return false;

		if (ItemPointerCompare(&slot->tts_tid, mintid) < 0)
		{
			ExecClearTuple(slot);
			if (ScanDirectionIsBackward(direction))
				return false;
			continue;
		}

		if (ItemPointerCompare(&slot->tts_tid, maxtid) > 0)
		{
			ExecClearTuple(slot);
			if (ScanDirectionIsForward(direction))
				return false;
			continue;
		}

		return true;
	}
}
