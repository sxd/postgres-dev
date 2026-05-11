/*-------------------------------------------------------------------------
 *
 * cs_slot.c
 *	  Custom TupleTableSlot implementation for columnstore tables.
 *
 * CSTupleTableSlot supports lazy column decompression.  For columnar
 * rows, the getsomeattrs callback decompresses only the columns that
 * the executor actually requests, avoiding I/O and CPU work for columns
 * that are never referenced by the query.
 *
 * Delta store rows are handled like virtual tuples -- all attributes
 * are filled eagerly by the scan, so getsomeattrs simply validates.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * contrib/columnstore/cs_slot.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"
#include "access/htup_details.h"
#include "executor/tuptable.h"
#include "utils/expandeddatum.h"
#include "utils/numeric.h"

static void tts_columnstore_init(TupleTableSlot *slot);
static void tts_columnstore_release(TupleTableSlot *slot);
static void tts_columnstore_clear(TupleTableSlot *slot);
static void tts_columnstore_getsomeattrs(TupleTableSlot *slot, int natts);
static Datum tts_columnstore_getsysattr(TupleTableSlot *slot, int attnum,
										bool *isnull);
static bool tts_columnstore_is_current_xact_tuple(TupleTableSlot *slot);
static void tts_columnstore_materialize(TupleTableSlot *slot);
static void tts_columnstore_copyslot(TupleTableSlot *dstslot,
									 TupleTableSlot *srcslot);
static HeapTuple tts_columnstore_copy_heap_tuple(TupleTableSlot *slot);
static MinimalTuple tts_columnstore_copy_minimal_tuple(TupleTableSlot *slot,
													   Size extra);

static void
tts_columnstore_init(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;

	csslot->data = NULL;
	csslot->cs_scan = NULL;
	csslot->cs_rel = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_row = 0;
	csslot->cs_is_columnar = false;
}

static void
tts_columnstore_release(TupleTableSlot *slot)
{
}

static void
tts_columnstore_clear(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;

	if (unlikely(TTS_SHOULDFREE(slot)))
	{
		pfree(csslot->data);
		csslot->data = NULL;
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	csslot->cs_scan = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_is_columnar = false;
	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
}

/*
 * Attribute materialization callback.
 *
 * All rows live in the delta heap in this commit, so every attribute was
 * filled eagerly when the tuple was stored; just mark them valid.  Columnar
 * rows get lazy per-column decompression here in a later commit.
 */
static void
tts_columnstore_getsomeattrs(TupleTableSlot *slot, int natts)
{
	slot->tts_nvalid = slot->tts_tupleDescriptor->natts;
}

static Datum
tts_columnstore_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot retrieve a system column in this context")));

	return 0;
}

static bool
tts_columnstore_is_current_xact_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("don't have transaction information for this type of tuple")));

	return false;
}

/*
 * Materialize the slot: copy all by-reference datums into slot's memory.
 * Must ensure all attributes are loaded first.
 */
static void
tts_columnstore_materialize(TupleTableSlot *slot)
{
	CSTupleTableSlot *csslot = (CSTupleTableSlot *) slot;
	TupleDesc	desc = slot->tts_tupleDescriptor;
	Size		sz = 0;
	char	   *data;
	int			natt;

	/* already materialized */
	if (TTS_SHOULDFREE(slot))
		return;

	/* ensure all columns are loaded */
	slot_getsomeattrs(slot, desc->natts);

	/* compute size of memory required */
	for (natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz += EOH_get_flat_size(DatumGetEOHP(val));
		}
		else
		{
			sz = att_nominal_alignby(sz, att->attalignby);
			sz = att_addlength_datum(sz, att->attlen, val);
		}
	}

	/* all data is byval */
	if (sz == 0)
	{
		/* Detach from scan so slot is self-contained */
		csslot->cs_scan = NULL;
		csslot->cs_colcache = NULL;
		csslot->cs_is_columnar = false;
		return;
	}

	/* allocate memory */
	csslot->data = data = MemoryContextAlloc(slot->tts_mcxt, sz);
	slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	/* copy all by-reference attributes into the pre-allocated space */
	for (natt = 0; natt < desc->natts; natt++)
	{
		CompactAttribute *att = TupleDescCompactAttr(desc, natt);
		Datum		val;

		if (att->attbyval || slot->tts_isnull[natt])
			continue;

		val = slot->tts_values[natt];

		if (att->attlen == -1 &&
			VARATT_IS_EXTERNAL_EXPANDED(DatumGetPointer(val)))
		{
			ExpandedObjectHeader *eoh = DatumGetEOHP(val);
			Size		data_length;

			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = EOH_get_flat_size(eoh);
			EOH_flatten_into(eoh, data, data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
		else
		{
			Size		data_length = 0;

			data = (char *) att_nominal_alignby(data, att->attalignby);
			data_length = att_addlength_datum(data_length, att->attlen, val);

			memcpy(data, DatumGetPointer(val), data_length);

			slot->tts_values[natt] = PointerGetDatum(data);
			data += data_length;
		}
	}

	/* Detach from scan so slot is self-contained */
	csslot->cs_scan = NULL;
	csslot->cs_colcache = NULL;
	csslot->cs_is_columnar = false;
}

static void
tts_columnstore_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;
	int			natt;

	tts_columnstore_clear(dstslot);

	slot_getallattrs(srcslot);

	for (natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* make sure storage doesn't depend on external memory */
	tts_columnstore_materialize(dstslot);
}

static HeapTuple
tts_columnstore_copy_heap_tuple(TupleTableSlot *slot)
{
	HeapTuple	tuple;

	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	tuple = heap_form_tuple(slot->tts_tupleDescriptor,
							slot->tts_values,
							slot->tts_isnull);

	/* Propagate TID so callers like ANALYZE's compare_rows can sort. */
	tuple->t_self = slot->tts_tid;
	tuple->t_tableOid = slot->tts_tableOid;

	return tuple;
}

static MinimalTuple
tts_columnstore_copy_minimal_tuple(TupleTableSlot *slot, Size extra)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull,
								   extra);
}

const TupleTableSlotOps TTSOpsColumnStore = {
	.base_slot_size = sizeof(CSTupleTableSlot),
	.init = tts_columnstore_init,
	.release = tts_columnstore_release,
	.clear = tts_columnstore_clear,
	.getsomeattrs = tts_columnstore_getsomeattrs,
	.getsysattr = tts_columnstore_getsysattr,
	.is_current_xact_tuple = tts_columnstore_is_current_xact_tuple,
	.materialize = tts_columnstore_materialize,
	.copyslot = tts_columnstore_copyslot,
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_columnstore_copy_heap_tuple,
	.copy_minimal_tuple = tts_columnstore_copy_minimal_tuple,
};
