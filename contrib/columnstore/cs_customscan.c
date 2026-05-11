/*-------------------------------------------------------------------------
 *
 * cs_customscan.c
 *	  CustomScan integration for columnstore tables.
 *
 * This file contains every plan-time path the columnstore extension
 * registers, and the executor methods that run them:
 *
 *   - ColumnstoreScan.  Replaces the planner-supplied SeqScan paths
 *     (serial and partial) for columnstore base relations, opens a
 *     TableScanDesc, and drives ExecScan against a virtual result
 *     slot containing only the projected columns.  Side-benefit: the
 *     projected target list also shrinks the MinimalTuple HashAgg
 *     would otherwise spill, which carries a null bitmap sized for
 *     every column of the (potentially very wide) table.
 *
 *   - ColumnstoreAggregate.  Recognises COUNT / SUM / AVG / MIN / MAX
 *     (with FILTER, GROUP BY, HAVING, WHERE) above a columnstore
 *     base relation and emits an upper-rel CustomScan that
 *     accumulates per-aggregate state directly inside the scan,
 *     finalising once per group.  Two stages dispatch: a sequential
 *     path on UPPERREL_GROUP_AGG for ungrouped queries, and a
 *     parallel-aware partial path on UPPERREL_PARTIAL_GROUP_AGG that
 *     each worker drives independently for the upstream Finalize
 *     Aggregate to combine.
 *
 *   - ColumnstoreLateMat.  At UPPERREL_ORDERED, defers reading
 *     non-qual / non-sort-key columns until after Sort + Limit has
 *     selected the surviving rows; the bottom probe scan emits
 *     (sort_key columns, packed_tid) and a top CustomScan refetches
 *     the payload columns via table_tuple_fetch_row_version.
 *
 * Path-level scaffolding (cs_set_rel_pathlist_hook,
 * cs_create_upper_paths_hook) and plan-tree walkers that derive
 * projection columns and scan keys from the surrounding plan also
 * live here, since they have no useful home in the table-AM file.
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 *
 * contrib/columnstore/cs_customscan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cs_internal.h"

#include "access/nbtree.h"
#include "access/parallel.h"
#include "access/relation.h"
#include "access/reloptions.h"
#include "access/relscan.h"
#include "access/tableam.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "executor/nodeSeqscan.h"
#include "lib/stringinfo.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "optimizer/cost.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "storage/shm_toc.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

/*
 * State carried between BeginCustomScan and EndCustomScan.
 */
typedef struct CSCustomScanState
{
	CustomScanState css;		/* must be first */
	TableScanDesc scandesc;
	bool		bloom_applied;	/* pushed-down bloom filters consumed yet? */
	bool		instr_flushed;	/* parallel instrumentation folded already? */
} CSCustomScanState;

/* Forward declarations */
static Plan *cs_find_plan_parent(Plan *root, Plan *target, bool *is_inner);
static Bitmapset *cs_extract_scan_needed_cols(Plan *root_plan,
											  Scan *scan_plan);
static int	cs_extract_scan_qual_keys(List *quals, Index scanrelid,
									  ScanKeyData **keys_out);
static bool rel_is_columnstore(Oid relid);
static void cs_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
									 Index rti, RangeTblEntry *rte);
static Plan *cs_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
							 struct CustomPath *best_path, List *tlist,
							 List *clauses, List *custom_plans);
static Node *cs_create_custom_scan_state(CustomScan *cscan);
static void cs_begin_custom_scan(CustomScanState *node, EState *estate,
								 int eflags);
static TupleTableSlot *cs_exec_custom_scan(CustomScanState *node);
static void cs_end_custom_scan(CustomScanState *node);
static void cs_rescan_custom_scan(CustomScanState *node);
static void cs_shutdown_custom_scan(CustomScanState *node);
static void cs_explain_custom_scan(CustomScanState *node, List *ancestors,
								   ExplainState *es);
static Size cs_estimate_dsm_custom_scan(CustomScanState *node,
										ParallelContext *pcxt);
static void cs_initialize_dsm_custom_scan(CustomScanState *node,
										  ParallelContext *pcxt,
										  void *coordinate);
static void cs_reinitialize_dsm_custom_scan(CustomScanState *node,
											ParallelContext *pcxt,
											void *coordinate);
static void cs_initialize_worker_custom_scan(CustomScanState *node,
											 shm_toc *toc, void *coordinate);

static TupleTableSlot *cs_custom_scan_next(CustomScanState *node);
static bool cs_custom_scan_recheck(CustomScanState *node,
								   TupleTableSlot *slot);
static void cs_apply_pushed_bloom_filters(CSCustomScanState *state);

/* Saved upstream hook chain pointer */
static set_rel_pathlist_hook_type prev_set_rel_pathlist_hook = NULL;

static const struct CustomPathMethods cs_path_methods = {
	.CustomName = "ColumnstoreScan",
	.PlanCustomPath = cs_path_to_plan,
};

static const struct CustomScanMethods cs_scan_methods = {
	.CustomName = "ColumnstoreScan",
	.CreateCustomScanState = cs_create_custom_scan_state,
};

static const struct CustomExecMethods cs_exec_methods = {
	.CustomName = "ColumnstoreScan",
	.BeginCustomScan = cs_begin_custom_scan,
	.ExecCustomScan = cs_exec_custom_scan,
	.EndCustomScan = cs_end_custom_scan,
	.ReScanCustomScan = cs_rescan_custom_scan,
	.EstimateDSMCustomScan = cs_estimate_dsm_custom_scan,
	.InitializeDSMCustomScan = cs_initialize_dsm_custom_scan,
	.ReInitializeDSMCustomScan = cs_reinitialize_dsm_custom_scan,
	.InitializeWorkerCustomScan = cs_initialize_worker_custom_scan,
	.ShutdownCustomScan = cs_shutdown_custom_scan,
	.ExplainCustomScan = cs_explain_custom_scan,
};

/*
 * cs_register_custom_scan_methods
 *
 * Idempotent registration of the CustomScan integration.  Called from
 * the extension's _PG_init.  When the extension is listed in
 * shared_preload_libraries (the recommended deployment for any setup
 * that runs parallel queries against columnstore tables), _PG_init
 * runs in the postmaster at server start, and every forked backend
 * -- regular client, parallel worker, autovacuum -- inherits the
 * registered methods table.  Without shared_preload_libraries,
 * _PG_init runs only when CREATE EXTENSION executes or when an SQL
 * function from the extension is first invoked, which can be after
 * parallel workers have already attempted to deserialise plans that
 * reference our CustomScan node tags.
 */
void
cs_register_custom_scan_methods(void)
{
	static bool registered = false;

	if (registered)
		return;

	RegisterCustomScanMethods(&cs_scan_methods);

	prev_set_rel_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = cs_set_rel_pathlist_hook;

	registered = true;
}

/* ------------------------------------------------------------------------
 * Path generation
 * ------------------------------------------------------------------------
 */

/*
 * Cached OID of the columnstore access method.  Looked up lazily on first
 * use because the AM does not exist until CREATE EXTENSION columnstore
 * has run, and re-resolved when pg_am changes (DROP EXTENSION /
 * CREATE EXTENSION cycles).
 */
static Oid	cs_am_oid_cache = InvalidOid;
static bool cs_am_oid_callback_registered = false;

static void
cs_am_oid_invalidate(Datum arg, int cacheid, uint32 hashvalue)
{
	cs_am_oid_cache = InvalidOid;
}

static Oid
cs_get_am_oid(void)
{
	if (!cs_am_oid_callback_registered)
	{
		CacheRegisterSyscacheCallback(AMOID, cs_am_oid_invalidate, (Datum) 0);
		cs_am_oid_callback_registered = true;
	}

	if (!OidIsValid(cs_am_oid_cache))
		cs_am_oid_cache = get_am_oid("columnstore", true);

	return cs_am_oid_cache;
}

/*
 * Test whether a relation oid refers to a table using the columnstore AM.
 * Uses the relation cache so the lookup is cheap; the planner already
 * holds these entries.
 */
static bool
rel_is_columnstore(Oid relid)
{
	HeapTuple	tuple;
	Form_pg_class pg_class;
	Oid			am_oid;
	bool		result;

	if (!OidIsValid(relid))
		return false;

	am_oid = cs_get_am_oid();
	if (!OidIsValid(am_oid))
		return false;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		return false;

	pg_class = (Form_pg_class) GETSTRUCT(tuple);

	/*
	 * The relkind check matters for partitioned tables: they may carry the
	 * columnstore relam (inherited by future partitions) but have no storage
	 * of their own, so no columnstore plan may ever scan one.  The planner
	 * usually keeps them away from us as append parents, but FROM ONLY
	 * produces a childless base rel that would otherwise pass.
	 */
	result = (pg_class->relam == am_oid &&
			  RELKIND_HAS_STORAGE(pg_class->relkind));
	ReleaseSysCache(tuple);
	return result;
}

/*
 * Build pathkeys describing the columnstore relation's natural row order
 * when its sort_key reloption guarantees globally sorted output.
 *
 * Returns NIL when the relation is not provably sorted across row groups.
 * The check is conservative: we only declare pathkeys when every row
 * group's zone-map [min, max] for the sort_key column is non-overlapping
 * and ascending across the row group directory.  This is the case when
 * data was inserted in sort order (the natural workload for sort_key,
 * e.g. time-series append-only), but not when random inserts produced
 * row groups with overlapping ranges.  Without the non-overlap property
 * the scan output is sorted only within each row group, not globally,
 * and declaring pathkeys would be a correctness bug.
 *
 * Limitations of this first cut: only the leading column of a
 * comma-separated sort_key is considered; only ASC NULLS LAST ordering
 * is recognised (matching cs_compact's sort).
 */
static List *
cs_try_build_sort_pathkeys(PlannerInfo *root, RelOptInfo *rel,
						   RangeTblEntry *rte)
{
	Relation	relation;
	const char *sort_key_str;
	char	   *first_col;
	char	   *comma;
	int			first_col_len;
	AttrNumber	sort_attno;
	Oid			sort_type;
	Oid			opclass;
	Oid			opfamily;
	Oid			lt_op;
	CSMetaPageData meta;
	BlockNumber *rg_blocks = NULL;
	bool		sorted = true;
	bool		any_seen = false;
	Datum		prev_max = (Datum) 0;
	bool		have_prev = false;
	FmgrInfo	cmp_finfo;
	Oid			cmp_proc;
	Oid			collation;
	List	   *pathkeys = NIL;
	int			natts;
	uint32		i;

	relation = relation_open(rte->relid, NoLock);

	sort_key_str = cs_get_sort_key(relation);
	if (sort_key_str == NULL || sort_key_str[0] == '\0')
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	/* Take only the leading column of a comma-separated sort_key */
	first_col = pstrdup(sort_key_str);
	while (*first_col == ' ')
		first_col++;
	comma = strchr(first_col, ',');
	if (comma)
		*comma = '\0';
	first_col_len = strlen(first_col);
	while (first_col_len > 0 && first_col[first_col_len - 1] == ' ')
		first_col[--first_col_len] = '\0';

	sort_attno = get_attnum(rte->relid, first_col);
	if (sort_attno == InvalidAttrNumber)
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	sort_type = TupleDescAttr(RelationGetDescr(relation),
							  sort_attno - 1)->atttypid;
	collation = TupleDescAttr(RelationGetDescr(relation),
							  sort_attno - 1)->attcollation;

	/* Default btree opfamily for the sort column's type */
	opclass = GetDefaultOpClass(sort_type, BTREE_AM_OID);
	if (!OidIsValid(opclass))
	{
		relation_close(relation, NoLock);
		return NIL;
	}
	opfamily = get_opclass_family(opclass);

	lt_op = get_opfamily_member(opfamily, sort_type, sort_type,
								BTLessStrategyNumber);
	cmp_proc = get_opfamily_proc(opfamily, sort_type, sort_type,
								 BTORDER_PROC);
	if (!OidIsValid(lt_op) || !OidIsValid(cmp_proc))
	{
		relation_close(relation, NoLock);
		return NIL;
	}
	fmgr_info(cmp_proc, &cmp_finfo);

	/*
	 * Walk the row group directory, reading each row group's zone map for the
	 * sort column.  Bail out as soon as we see a row group with no zone map
	 * (e.g. all NULLs) or a min that does not exceed the previous row group's
	 * max.
	 */
	natts = RelationGetDescr(relation)->natts;

	if (RelationGetNumberOfBlocks(relation) > 0)
	{
		cs_read_metapage(relation, &meta);

		/*
		 * Any delta pages defeat the ordering claim: the scan emits delta
		 * rows first, in insertion order, before the (ordered) row groups.
		 * The insert paths send a relcache invalidation when the delta
		 * transitions from empty to non-empty, so cached plans that relied on
		 * an empty delta are replanned rather than silently misordered.
		 */
		if (meta.cs_delta_nblocks > 0)
		{
			relation_close(relation, NoLock);
			return NIL;
		}
		if (meta.cs_nrowgroups > 0)
			rg_blocks = cs_read_rgdir(relation, &meta);
	}
	if (rg_blocks == NULL)
	{
		relation_close(relation, NoLock);
		return NIL;
	}

	{
		Size		rg_size = CSRowGroupDescSize(natts);
		CSRowGroupDesc *rg_desc = palloc(rg_size);

		for (i = 0; i < meta.cs_nrowgroups && sorted; i++)
		{
			CSZoneMap  *zm;

			if (rg_blocks[i] == InvalidBlockNumber)
				continue;

			cs_read_rowgroup_catalog(relation, rg_blocks[i], rg_desc, natts);

			if (rg_desc->rg_num_rows == 0)
				continue;

			zm = &CSRowGroupGetZoneMaps(rg_desc)[sort_attno - 1];
			if (!zm->zm_has_minmax || zm->zm_mode != CS_ZM_BYVAL)
			{
				/*
				 * No zone map (all NULLs) or non-by-value mode (text /
				 * sort-key encoded).  We do not currently extend the
				 * non-overlap check to byref types, so bail.
				 */
				sorted = false;
				break;
			}

			if (have_prev)
			{
				int			cmp;

				/*
				 * Need this row group's min strictly greater than the
				 * previous row group's max.  Equality is not enough -- row
				 * groups with equal boundary values violate strict
				 * non-overlap, which the planner relies on to skip an upper
				 * sort.
				 */
				cmp = DatumGetInt32(FunctionCall2Coll(&cmp_finfo, collation,
													  prev_max, zm->zm_min));
				if (cmp >= 0)
					sorted = false;
			}

			prev_max = zm->zm_max;
			have_prev = true;
			any_seen = true;
		}
		pfree(rg_desc);
	}

	if (sorted && any_seen)
	{
		Var		   *var;

		var = makeVar(rel->relid, sort_attno, sort_type, -1, collation, 0);
		pathkeys = build_expression_pathkey(root, (Expr *) var,
											lt_op, rel->relids, false);
	}

	relation_close(relation, NoLock);
	return pathkeys;
}

/*
 * Hook entry point.  For each base relation that uses the columnstore AM,
 * REPLACE the planner-supplied SeqScan paths (both serial and parallel)
 * with a single CustomScan path.  We have to replace rather than just
 * add-alongside because there is no AM-level cost-factor callback that
 * could scale the standard scan-path costs, so the planner can't be
 * relied on to pick our CustomScan over the built-in SeqScan paths.
 *
 * The CustomPath inherits the standard rel->reltarget so the
 * planner-supplied targetlist passed to cs_path_to_plan reflects only
 * the columns referenced upstream.  Costs are inherited from the
 * cheapest standard path; the per-tuple portion (total - startup) is
 * scaled by the AM's cpu_tuple_cost_factor (0.5 today) to reflect the
 * cheaper per-tuple work of a column-projected scan.  Other factors
 * cs_relation_cost_factors returns -- page-cost scalars, random-I/O
 * page count, index-fetch cost -- have no consumer in this hook and
 * are ignored.  When the relation has a sort_key reloption and every
 * row group's zone map is non-overlapping ascending, the path also
 * advertises pathkeys so upper plan nodes can skip an explicit Sort.
 */
static void
cs_set_rel_pathlist_hook(PlannerInfo *root, RelOptInfo *rel,
						 Index rti, RangeTblEntry *rte)
{
	CustomPath *cpath;
	Path	   *baseline = NULL;
	List	   *new_pathlist = NIL;
	List	   *new_partial_pathlist = NIL;
	ListCell   *lc;
	double		seq_factor = 1.0;
	double		rand_factor = 1.0;
	double		cpu_factor = 1.0;
	BlockNumber rand_io_pages = 0;
	bool		disk_par = false;
	double		index_fetch_cost = 0.0;
	Relation	relation;
	Relation	rel_open;
	int			num_workers;

	if (prev_set_rel_pathlist_hook)
		prev_set_rel_pathlist_hook(root, rel, rti, rte);

	if (rte->rtekind != RTE_RELATION)
		return;

	/*
	 * Handle plain base rels and partition/inheritance children alike.
	 * Skipping children would leave partitions on the planner's stock SeqScan
	 * paths -- losing projection and qual pushdown, and, worse, letting
	 * ExecSupportsBackwardScan() approve a SCROLL cursor over a path whose
	 * executor cannot actually scan backwards.
	 */
	if (rel->reloptkind != RELOPT_BASEREL &&
		rel->reloptkind != RELOPT_OTHER_MEMBER_REL)
		return;
	if (!rel_is_columnstore(rte->relid))
		return;

	/*
	 * Walk rel->pathlist: find the cheapest SeqScan path to use as our
	 * row-estimate baseline, and rebuild the list without the SeqScan paths.
	 * Keep IndexScan / BitmapHeap / TidScan paths as-is.
	 */
	foreach(lc, rel->pathlist)
	{
		Path	   *p = (Path *) lfirst(lc);

		if (p->pathtype == T_SeqScan)
		{
			if (p->param_info == NULL &&
				(baseline == NULL || p->total_cost < baseline->total_cost))
				baseline = p;
			continue;
		}
		new_pathlist = lappend(new_pathlist, p);
	}
	if (baseline == NULL)
		return;

	rel->pathlist = new_pathlist;

	/* Same for partial_pathlist (parallel SeqScan candidates). */
	foreach(lc, rel->partial_pathlist)
	{
		Path	   *p = (Path *) lfirst(lc);

		if (p->pathtype != T_SeqScan)
			new_partial_pathlist = lappend(new_partial_pathlist, p);
	}
	rel->partial_pathlist = new_partial_pathlist;

	/* Apply the AM's cost-factor helper on top of baseline. */
	relation = relation_open(rte->relid, NoLock);
	cs_relation_cost_factors(relation,
							 &seq_factor, &rand_factor, &cpu_factor,
							 &rand_io_pages, &disk_par,
							 &index_fetch_cost);
	relation_close(relation, NoLock);

	cpath = makeNode(CustomPath);
	cpath->path.pathtype = T_CustomScan;
	cpath->path.parent = rel;
	cpath->path.pathtarget = rel->reltarget;
	cpath->path.param_info = NULL;
	cpath->path.parallel_aware = false;
	cpath->path.parallel_safe = baseline->parallel_safe;
	cpath->path.parallel_workers = 0;
	cpath->path.rows = baseline->rows;
	cpath->path.startup_cost = baseline->startup_cost;
	cpath->path.total_cost = baseline->startup_cost +
		(baseline->total_cost - baseline->startup_cost) * cpu_factor;
	cpath->path.pathkeys = cs_try_build_sort_pathkeys(root, rel, rte);

	cpath->custom_paths = NIL;
	cpath->custom_private = NIL;
	cpath->methods = &cs_path_methods;

	/*
	 * Advertise that this scan can consume a hash-join bloom filter pushed
	 * down to it.  The planner (find_bloom_filter_recipient) only targets a
	 * CustomScan that sets this, and the executor applies the filter inside
	 * the scan loop -- at row-group / dictionary granularity -- via
	 * cs_apply_pushed_bloom_filters().  Left off the partial (parallel) path
	 * below, since filter pushdown is serial-only for now.
	 */
	cpath->flags = CUSTOMPATH_SUPPORT_BLOOM_FILTERS;

	add_path(rel, &cpath->path);

	/*
	 * Also generate a partial (parallel-aware) CustomPath when the relation
	 * is large enough to benefit and the AM allows parallelism.  The planner
	 * will wrap this in a Gather above any aggregate or join that sits over
	 * the scan; without it we'd lose every parallel-scan win.
	 */
	rel_open = relation_open(rte->relid, NoLock);
	num_workers = cs_compute_parallel_workers(rel_open);
	relation_close(rel_open, NoLock);

	if (num_workers > 0 && baseline->parallel_safe)
	{
		CustomPath *ppath = makeNode(CustomPath);
		double		divisor = (double) num_workers;

		ppath->path.pathtype = T_CustomScan;
		ppath->path.parent = rel;
		ppath->path.pathtarget = rel->reltarget;
		ppath->path.param_info = NULL;
		ppath->path.parallel_aware = true;
		ppath->path.parallel_safe = true;
		ppath->path.parallel_workers = num_workers;
		ppath->path.rows = clamp_row_est(baseline->rows / divisor);
		ppath->path.startup_cost = baseline->startup_cost;
		ppath->path.total_cost = baseline->startup_cost +
			((baseline->total_cost - baseline->startup_cost) * cpu_factor)
			/ divisor;
		ppath->path.pathkeys = NIL;

		ppath->custom_paths = NIL;
		ppath->custom_private = NIL;
		ppath->methods = &cs_path_methods;
		ppath->flags = 0;

		add_partial_path(rel, &ppath->path);
	}
}

/*
 * Convert a CustomPath into a CustomScan plan node.  The planner has
 * already projected the tlist down to upstream-referenced columns
 * (use_physical_tlist is bypassed for CustomPath, see createplan.c), so
 * we simply install it as the scan's output and let the standard
 * executor projection machinery deliver a narrow virtual slot upstream.
 */
static Plan *
cs_path_to_plan(PlannerInfo *root, RelOptInfo *rel,
				struct CustomPath *best_path, List *tlist,
				List *clauses, List *custom_plans)
{
	CustomScan *cscan = makeNode(CustomScan);
	List	   *quals;

	quals = extract_actual_clauses(clauses, false);

	cscan->scan.plan.targetlist = tlist;
	cscan->scan.plan.qual = quals;
	cscan->scan.plan.lefttree = NULL;
	cscan->scan.plan.righttree = NULL;
	cscan->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cscan->scan.plan.parallel_safe = best_path->path.parallel_safe;
	cscan->scan.scanrelid = rel->relid;

	cscan->flags = best_path->flags;
	cscan->custom_plans = NIL;
	cscan->custom_exprs = NIL;
	cscan->custom_private = NIL;
	cscan->custom_scan_tlist = NIL;
	cscan->custom_relids = bms_make_singleton(rel->relid);
	cscan->methods = &cs_scan_methods;

	return (Plan *) cscan;
}

/* ------------------------------------------------------------------------
 * Execution callbacks
 * ------------------------------------------------------------------------
 */

static Node *
cs_create_custom_scan_state(CustomScan *cscan)
{
	CSCustomScanState *state = palloc0(sizeof(CSCustomScanState));

	NodeSetTag(state, T_CustomScanState);
	state->css.methods = &cs_exec_methods;
	state->css.slotOps = &TTSOpsColumnStore;
	state->scandesc = NULL;
	return (Node *) state;
}

/*
 * Helper: open the AM-level TableScanDesc and forward both projection and
 * scan-key information.  Used both by serial cs_begin_custom_scan and by
 * the parallel-worker initializer.  pscan==NULL means a non-parallel scan.
 */
static void
cs_open_scan(CSCustomScanState *state, EState *estate,
			 ParallelTableScanDesc pscan)
{
	Relation	rel = state->css.ss.ss_currentRelation;
	CustomScan *cscan = (CustomScan *) state->css.ss.ps.plan;
	Bitmapset  *needed_cols;
	ScanKeyData *qual_keys;
	int			nqual_keys;

	/*
	 * No user flags: table_beginscan() and table_beginscan_parallel() both
	 * supply the seqscan-style internal flags (SO_TYPE_SEQSCAN and the
	 * SO_ALLOW_* set) themselves.
	 */
	if (pscan != NULL)
		state->scandesc = table_beginscan_parallel(rel, pscan, SO_NONE);
	else
		state->scandesc = table_beginscan(rel, estate->es_snapshot,
										  0, NULL, SO_NONE);

	/*
	 * Publish the scan in the embedded ScanState so generic code paths that
	 * look up a base-rel ScanState's TableScanDesc -- such as the hash-join
	 * bloom-filter pushdown machinery -- can find it.
	 */
	state->css.ss.ss_currentScanDesc = state->scandesc;

	needed_cols = cs_extract_scan_needed_cols(estate->es_plannedstmt->planTree,
											  &cscan->scan);
	if (needed_cols != NULL)
	{
		cs_scan_set_projection(state->scandesc, needed_cols);
		bms_free(needed_cols);
	}

	nqual_keys = cs_extract_scan_qual_keys(cscan->scan.plan.qual,
										   cscan->scan.scanrelid,
										   &qual_keys);
	if (nqual_keys > 0)
	{
		cs_scan_set_qual_keys(state->scandesc, nqual_keys, qual_keys);
		pfree(qual_keys);
	}
}

static void
cs_begin_custom_scan(CustomScanState *node, EState *estate, int eflags)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	Assert(node->ss.ss_currentRelation != NULL);

	/*
	 * ExecInitCustomScan opened the relation, set up ss_ScanTupleSlot using
	 * TTSOpsColumnStore, and assigned ProjInfo because the plan targetlist
	 * differs from the scan tupledesc (use_physical_tlist is bypassed for
	 * CustomPath).  ps_ResultTupleSlot is a TTSOpsVirtual slot with the
	 * projected tupledesc; that is the slot that flows upstream from
	 * ExecCustomScan -> ExecScan.
	 *
	 * For non-parallel scans, open the underlying TableScanDesc here.  For
	 * parallel scans, the leader's InitializeDSMCustomScan and each worker's
	 * InitializeWorkerCustomScan open their own scan over a shared
	 * ParallelTableScanDesc; we leave state->scandesc NULL here and let the
	 * DSM callbacks fill it in.
	 */
	state->scandesc = NULL;

	/* plain EXPLAIN never reads the table; don't open the scan */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	if (node->ss.ps.state->es_use_parallel_mode &&
		node->ss.ps.plan->parallel_aware)
	{
		/* DSM callbacks will open the scan */
		return;
	}

	cs_open_scan(state, estate, NULL);
}

static Size
cs_estimate_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt)
{
	Relation	rel = node->ss.ss_currentRelation;
	EState	   *estate = node->ss.ps.state;

	/*
	 * Go through the table_parallelscan_* wrappers, not the bare AM callback:
	 * they account for and serialize the scan snapshot into the shared
	 * descriptor, which table_beginscan_parallel() in each worker restores.
	 * Calling the AM callback directly leaves phs_snapshot_off zero, and the
	 * workers would "restore" the descriptor header itself as their snapshot.
	 */
	return table_parallelscan_estimate(rel, estate->es_snapshot);
}

static void
cs_initialize_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt,
							  void *coordinate)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;
	EState	   *estate = node->ss.ps.state;

	/* see cs_estimate_dsm_custom_scan for why the wrapper is required */
	table_parallelscan_initialize(rel, pscan, estate->es_snapshot);
	cs_open_scan(state, estate, pscan);
}

static void
cs_reinitialize_dsm_custom_scan(CustomScanState *node, ParallelContext *pcxt,
								void *coordinate)
{
	Relation	rel = node->ss.ss_currentRelation;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	table_parallelscan_reinitialize(rel, pscan);
}

static void
cs_initialize_worker_custom_scan(CustomScanState *node, shm_toc *toc,
								 void *coordinate)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	ParallelTableScanDesc pscan = (ParallelTableScanDesc) coordinate;

	cs_open_scan(state, node->ss.ps.state, pscan);
}

static TupleTableSlot *
cs_exec_custom_scan(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) cs_custom_scan_next,
					(ExecScanRecheckMtd) cs_custom_scan_recheck);
}

/*
 * Translate the bloom filters the planner pushed onto this CustomScan into
 * per-column entries on the TableScanDesc, so the scan loop can prune row
 * groups / dictionary entries.
 *
 * The filters live on the producing HashJoin's Hash node and are built lazily;
 * HashJoin.bloom_eager (set because we advertised CUSTOMPATH_SUPPORT_BLOOM_FILTERS)
 * guarantees the build happens before our first getnextslot, so they are ready
 * here.  We map each join key to a column and to its outer hash function, and
 * register it with cs_scan_set_bloom_filter() for hash32 probing.
 *
 * For a single-key join the combined filter is exactly that key's hash filter,
 * so we use it directly.  For a multi-key join we use the per-key filters
 * (HashJoin.bloom_perkey, also implied by our flag), since the combined filter
 * mixes the keys and cannot be tested against one column's dictionary.  A
 * producer whose filter is not yet built is skipped (no pruning, still
 * correct).
 */
static void
cs_apply_pushed_bloom_filters(CSCustomScanState *state)
{
	CustomScanState *node = &state->css;
	EState	   *estate = node->ss.ps.state;
	MemoryContext oldcxt;
	ListCell   *lc;

	if (node->ss.ps.bloom_filters == NIL || state->scandesc == NULL)
		return;

	oldcxt = MemoryContextSwitchTo(estate->es_query_cxt);

	foreach(lc, node->ss.ps.bloom_filters)
	{
		BloomFilterState *bfs = (BloomFilterState *) lfirst(lc);
		HashState  *hash;
		BloomFilter *bf = bfs->filter;
		int			nkeys = list_length(bf->filter_exprs);

		if (bfs->producer == NULL)
			continue;
		hash = castNode(HashState, innerPlanState(&bfs->producer->js.ps));

		if (nkeys == 1)
		{
			Var		   *var = (Var *) linitial(bf->filter_exprs);
			Oid			outfn,
						innfn;

			/* combined filter == the single key's hash filter */
			if (hash->bloom_filter == NULL || !IsA(var, Var))
				continue;
			get_op_hash_functions(linitial_oid(bf->hashops), &outfn, &innfn);
			cs_scan_set_bloom_filter(state->scandesc, var->varattno,
									 hash->bloom_filter, outfn,
									 linitial_oid(bf->hashcollations));
		}
		else if (hash->perkey_filters != NULL &&
				 hash->perkey_nfilters == nkeys)
		{
			for (int k = 0; k < nkeys; k++)
			{
				Var		   *var = (Var *) list_nth(bf->filter_exprs, k);
				Oid			outfn,
							innfn;

				if (!IsA(var, Var))
					continue;
				get_op_hash_functions(list_nth_oid(bf->hashops, k),
									  &outfn, &innfn);
				cs_scan_set_bloom_filter(state->scandesc, var->varattno,
										 hash->perkey_filters[k], outfn,
										 list_nth_oid(bf->hashcollations, k));
			}
		}
	}

	MemoryContextSwitchTo(oldcxt);
}

static TupleTableSlot *
cs_custom_scan_next(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	if (state->scandesc == NULL)
		return NULL;

	/*
	 * On the first tuple request, consume any bloom filters the planner
	 * pushed down (by now the producing hash table, and thus the filters, are
	 * built).
	 */
	if (!state->bloom_applied)
	{
		cs_apply_pushed_bloom_filters(state);
		state->bloom_applied = true;
	}

	if (table_scan_getnextslot(state->scandesc, ForwardScanDirection, slot))
		return slot;
	return NULL;
}

static bool
cs_custom_scan_recheck(CustomScanState *node, TupleTableSlot *slot)
{
	/*
	 * No EPQ rechecks: cstore is not subject to concurrent updates that could
	 * change visibility mid-scan.
	 */
	return true;
}

static void
cs_end_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	if (state->scandesc != NULL)
	{
		table_endscan(state->scandesc);
		state->scandesc = NULL;
	}
}

static void
cs_rescan_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;

	/* re-arm parallel instrumentation folding (shared totals reset in DSM) */
	state->instr_flushed = false;

	if (state->scandesc != NULL)
	{
		/*
		 * Drop the pushed-down bloom filters: the producing hash table (and
		 * thus the filter) is rebuilt across a rescan, so the pointers we
		 * registered are stale.  cs_apply_pushed_bloom_filters() re-pulls the
		 * fresh ones on the next getnextslot.
		 */
		cs_scan_set_bloom_filter(state->scandesc, 0, NULL,
								 InvalidOid, InvalidOid);
		state->bloom_applied = false;

		table_rescan(state->scandesc, NULL);
	}
}

/*
 * Shutdown callback: fold this participant's zone-map pruning counters into
 * the shared totals so the leader's EXPLAIN reports the whole scan, not just
 * the work units it personally claimed.
 *
 * ExecShutdownNode visits this node (a child of Gather) before it tears the
 * Gather's parallel context down, and a worker's shutdown runs before it
 * signals EOF -- so by the time the leader's shutdown runs, after Gather has
 * consumed every worker's output, all workers have already contributed.
 * Workers only add; the leader additionally folds the shared total into its
 * own scan, which cs_explain_custom_scan then reads.
 *
 * Only the zone-map counters need this: a hash-join bloom filter is never
 * pushed to a parallel-aware scan (see try_push_bloom_filter), so the bloom
 * counters are zero in every participant under parallelism.
 */
static void
cs_shutdown_custom_scan(CustomScanState *node)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	CScanDesc	scan;
	CSParallelScanDesc cpscan;

	if (state->scandesc == NULL || state->instr_flushed)
		return;
	scan = (CScanDesc) state->scandesc;
	cpscan = (CSParallelScanDesc) scan->rs_base.rs_parallel;
	if (cpscan == NULL)
		return;					/* serial scan: private counters suffice */

	state->instr_flushed = true;

	if (IsParallelWorker())
	{
		pg_atomic_fetch_add_u64(&cpscan->pcs_instr_rg_examined,
								scan->instr_rg_examined);
		pg_atomic_fetch_add_u64(&cpscan->pcs_instr_rg_zonemap_skipped,
								scan->instr_rg_zonemap_skipped);
	}
	else
	{
		/* leader: add the workers' totals to its own for the EXPLAIN lines */
		scan->instr_rg_examined +=
			pg_atomic_read_u64(&cpscan->pcs_instr_rg_examined);
		scan->instr_rg_zonemap_skipped +=
			pg_atomic_read_u64(&cpscan->pcs_instr_rg_zonemap_skipped);
	}
}

static void
cs_explain_custom_scan(CustomScanState *node, List *ancestors,
					   ExplainState *es)
{
	CSCustomScanState *state = (CSCustomScanState *) node;
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	EState	   *estate = node->ss.ps.state;
	Bitmapset  *needed_cols;
	StringInfoData buf;
	int			attno = -1;
	bool		first = true;

	/*
	 * Report row-group pruning effectiveness when EXPLAIN ANALYZE is in use
	 * and at least one row-group catalog was examined.  Both counters live on
	 * the underlying TableScanDesc and remain accessible until ExecEnd runs;
	 * ExecutorEnd is called after ExplainPrintPlan finishes.
	 */
	if (es->analyze && state->scandesc != NULL)
	{
		CScanDesc	scan = (CScanDesc) state->scandesc;

		if (scan->instr_rg_examined > 0)
		{
			ExplainPropertyInteger("Row Groups Examined", NULL,
								   scan->instr_rg_examined, es);
			ExplainPropertyInteger("Row Groups Skipped by Zone Map", NULL,
								   scan->instr_rg_zonemap_skipped, es);
		}

		/*
		 * Report the effect of any hash-join bloom filter pushed down to this
		 * scan.  These lines account for the gap between the planner's
		 * estimated row count (computed before the filter was attached) and
		 * the actual rows emitted: rows the filter removed never reach an
		 * upstream node, and whole row groups it eliminated are never
		 * decompressed.  When the filter proved ineffective we disabled it
		 * mid-scan and say so, so the estimate/actual divergence is not
		 * misread as a planner misestimate.
		 */
		if (scan->instr_bloom_rg_probed > 0)
		{
			ExplainPropertyInteger("Rows Removed by Bloom Filter", NULL,
								   scan->instr_bloom_rows_removed, es);
			ExplainPropertyInteger("Row Groups Skipped by Bloom Filter", NULL,
								   scan->instr_bloom_rg_skipped, es);
			if (scan->bf_disabled)
				ExplainPropertyText("Bloom Filter Probing",
									"disabled (ineffective)", es);
		}
	}

	if (rel == NULL)
		return;

	needed_cols = cs_extract_scan_needed_cols(estate->es_plannedstmt->planTree,
											  &cscan->scan);
	if (needed_cols == NULL)
		return;

	initStringInfo(&buf);
	while ((attno = bms_next_member(needed_cols, attno)) >= 0)
	{
		Form_pg_attribute attr;

		if (attno < 0 || attno >= RelationGetDescr(rel)->natts)
			continue;
		attr = TupleDescAttr(RelationGetDescr(rel), attno);
		if (!first)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, NameStr(attr->attname));
		first = false;
	}

	if (buf.len > 0)
		ExplainPropertyText("Columnstore Projected Columns", buf.data, es);

	pfree(buf.data);
	bms_free(needed_cols);
}

/* ================================================================
 * Plan-tree walkers used to derive projection columns and scan keys
 * for a columnstore base relation from the surrounding plan, since
 * the AM has no direct planner-side hooks for either.
 * ==============================================================
 */

/*
 * find_plan_parent
 *		Walk the plan tree to find the immediate parent of `target`.
 *
 * Returns the parent Plan node and sets *is_inner to true if `target`
 * is the inner (right) child.  Returns NULL if `target` is the root
 * or not found.
 */
static Plan *
cs_find_plan_parent(Plan *root, Plan *target, bool *is_inner)
{
	Plan	   *result;

	if (root == NULL)
		return NULL;
	if (root->lefttree == target)
	{
		*is_inner = false;
		return root;
	}
	if (root->righttree == target)
	{
		*is_inner = true;
		return root;
	}

	/* Check Append/MergeAppend/SubqueryScan children */
	if (IsA(root, Append))
	{
		ListCell   *lc;

		foreach(lc, ((Append *) root)->appendplans)
		{
			if ((Plan *) lfirst(lc) == target)
			{
				*is_inner = false;
				return root;
			}
		}
	}
	else if (IsA(root, MergeAppend))
	{
		ListCell   *lc;

		foreach(lc, ((MergeAppend *) root)->mergeplans)
		{
			if ((Plan *) lfirst(lc) == target)
			{
				*is_inner = false;
				return root;
			}
		}
	}
	else if (IsA(root, SubqueryScan))
	{
		if (((SubqueryScan *) root)->subplan == target)
		{
			*is_inner = false;
			return root;
		}
	}

	/* Recurse into lefttree / righttree */
	result = cs_find_plan_parent(root->lefttree, target, is_inner);
	if (result != NULL)
		return result;
	result = cs_find_plan_parent(root->righttree, target, is_inner);
	if (result != NULL)
		return result;

	/* Recurse into Append/MergeAppend/SubqueryScan children */
	if (IsA(root, Append))
	{
		ListCell   *lc;

		foreach(lc, ((Append *) root)->appendplans)
		{
			result = cs_find_plan_parent((Plan *) lfirst(lc), target, is_inner);
			if (result != NULL)
				return result;
		}
	}
	else if (IsA(root, MergeAppend))
	{
		ListCell   *lc;

		foreach(lc, ((MergeAppend *) root)->mergeplans)
		{
			result = cs_find_plan_parent((Plan *) lfirst(lc), target, is_inner);
			if (result != NULL)
				return result;
		}
	}
	else if (IsA(root, SubqueryScan))
	{
		result = cs_find_plan_parent(((SubqueryScan *) root)->subplan,
									 target, is_inner);
		if (result != NULL)
			return result;
	}

	return NULL;
}

/*
 * extract_scan_needed_cols
 *		Determine which table columns are needed by the scan and its parent.
 *
 * Returns a Bitmapset of 0-indexed column numbers, or NULL if no columns
 * are needed (e.g. COUNT(*)).  The caller is responsible for freeing the
 * returned Bitmapset.
 *
 * The algorithm:
 * 1. Collect columns referenced in the scan's own qual.
 * 2. Find the parent plan node (via find_plan_parent).
 * 3. Determine which output positions the parent actually uses.
 * 4. Map those positions back to base-table column numbers via the
 *    scan's targetlist.
 * 5. Convert from pull_varattnos encoding to 0-indexed column numbers.
 */
static Bitmapset *
cs_extract_scan_needed_cols(Plan *root_plan, Scan *scan_plan)
{
	Plan	   *parent;
	bool		is_inner = false;
	Index		ref_varno;
	Bitmapset  *ref_attrs = NULL;
	Bitmapset  *needed_cols = NULL;
	int			attno;

	/* Include columns referenced in the scan's own qual */
	if (scan_plan->plan.qual != NIL)
		pull_varattnos((Node *) scan_plan->plan.qual,
					   scan_plan->scanrelid, &ref_attrs);

	/* Find the parent node to discover which output positions it uses */
	parent = cs_find_plan_parent(root_plan, &scan_plan->plan, &is_inner);
	if (parent == NULL)
	{
		/*
		 * Scan is the root plan.  Include the scan's own targetlist which
		 * contains the output column Vars (these still use the real varno,
		 * unlike parent nodes where setrefs.c rewrites them to OUTER_VAR).
		 */
		pull_varattnos((Node *) scan_plan->plan.targetlist,
					   scan_plan->scanrelid, &ref_attrs);
		if (ref_attrs == NULL)
			return NULL;
	}
	else
	{
		Bitmapset  *outer_refs = NULL;
		List	   *scan_tlist = scan_plan->plan.targetlist;
		int			pos_encoded;

		ref_varno = is_inner ? INNER_VAR : OUTER_VAR;

		/*
		 * ModifyTable (INSERT/UPDATE/DELETE) consumes all columns from its
		 * input but doesn't express this through Var references.  Return all
		 * scan columns as needed.
		 */
		if (IsA(parent, ModifyTable))
		{
			ListCell   *lc;

			foreach(lc, scan_plan->plan.targetlist)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);

				pull_varattnos((Node *) tle->expr,
							   scan_plan->scanrelid, &ref_attrs);
			}
			goto convert_attrs;
		}

		pull_varattnos((Node *) parent->targetlist,
					   ref_varno, &outer_refs);
		pull_varattnos((Node *) parent->qual,
					   ref_varno, &outer_refs);

		/*
		 * Join nodes store condition expressions in separate fields that
		 * aren't in plan.targetlist or plan.qual.  Extract column references
		 * from those too.
		 */
		if (IsA(parent, NestLoop) || IsA(parent, HashJoin) ||
			IsA(parent, MergeJoin))
		{
			Join	   *join = (Join *) parent;

			pull_varattnos((Node *) join->joinqual,
						   ref_varno, &outer_refs);
		}
		if (IsA(parent, NestLoop))
		{
			NestLoop   *nl = (NestLoop *) parent;
			ListCell   *lc;

			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);

				pull_varattnos((Node *) nlp->paramval,
							   ref_varno, &outer_refs);
			}
		}
		else if (IsA(parent, HashJoin))
		{
			HashJoin   *hj = (HashJoin *) parent;

			pull_varattnos((Node *) hj->hashclauses,
						   ref_varno, &outer_refs);
		}
		else if (IsA(parent, MergeJoin))
		{
			MergeJoin  *mj = (MergeJoin *) parent;

			pull_varattnos((Node *) mj->mergeclauses,
						   ref_varno, &outer_refs);
		}

		/*
		 * Some nodes reference input columns by positional index arrays
		 * rather than Var expressions.  Add those too.
		 */
		if (IsA(parent, Agg))
		{
			Agg		   *agg = (Agg *) parent;

			for (int i = 0; i < agg->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											agg->grpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, Group))
		{
			Group	   *grp = (Group *) parent;

			for (int i = 0; i < grp->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											grp->grpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, WindowAgg))
		{
			WindowAgg  *wag = (WindowAgg *) parent;

			for (int i = 0; i < wag->partNumCols; i++)
				outer_refs = bms_add_member(outer_refs,
											wag->partColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
			for (int i = 0; i < wag->ordNumCols; i++)
				outer_refs = bms_add_member(outer_refs,
											wag->ordColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, Unique))
		{
			Unique	   *uniq = (Unique *) parent;

			for (int i = 0; i < uniq->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											uniq->uniqColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}
		else if (IsA(parent, SetOp))
		{
			SetOp	   *setop = (SetOp *) parent;

			for (int i = 0; i < setop->numCols; i++)
				outer_refs = bms_add_member(outer_refs,
											setop->cmpColIdx[i] -
											FirstLowInvalidHeapAttributeNumber);
		}

		/*
		 * Map parent's output-position references back to base table columns
		 * via the scan's targetlist.  For a physical tlist (output position
		 * == attribute number) this is an identity mapping.  For a
		 * non-physical tlist (e.g. SELECT col3 FROM t) the parent's OUTER_VAR
		 * attno 1 maps to the scan's first target entry, whose Var gives the
		 * real base table column.
		 */
		pos_encoded = -1;
		while ((pos_encoded = bms_next_member(outer_refs, pos_encoded)) >= 0)
		{
			int			pos = pos_encoded + FirstLowInvalidHeapAttributeNumber;

			if (pos > 0 && pos <= list_length(scan_tlist))
			{
				TargetEntry *tle = (TargetEntry *) list_nth(scan_tlist,
															pos - 1);

				pull_varattnos((Node *) tle->expr,
							   scan_plan->scanrelid, &ref_attrs);
			}
		}
		bms_free(outer_refs);
	}

convert_attrs:
	if (ref_attrs == NULL)
		return NULL;

	/*
	 * Convert from pull_varattnos offset encoding to 0-indexed column
	 * numbers.
	 */
	attno = -1;
	while ((attno = bms_next_member(ref_attrs, attno)) >= 0)
	{
		int			real_attno = attno + FirstLowInvalidHeapAttributeNumber;

		/*
		 * A whole-row Var (attno 0) references every column; projection
		 * pushdown must be skipped or the unreferenced fields of the
		 * composite come back NULL for columnar rows.
		 */
		if (real_attno == 0)
		{
			bms_free(ref_attrs);
			bms_free(needed_cols);
			return NULL;
		}
		if (real_attno > 0)
			needed_cols = bms_add_member(needed_cols, real_attno - 1);
	}
	bms_free(ref_attrs);

	return needed_cols;
}


/* ----------------------------------------------------------------
 *		Scan qual key extraction support
 * ----------------------------------------------------------------
 */

/*
 * extract_scan_qual_keys
 *		Extract btree-strategy scan keys from a list of qual expressions.
 *
 * Walks the qual list looking for simple "Var op Const" comparisons,
 * ScalarArrayOpExpr (IN-list), and NullTest expressions.  For each one
 * that can be mapped to a btree strategy number, fills in a ScanKeyData
 * entry.
 *
 * Returns the number of extracted keys.  If > 0, *keys_out is set to
 * a palloc'd array of ScanKeyData entries.  Caller must pfree it.
 */
static int
cs_extract_scan_qual_keys(List *quals, Index scanrelid, ScanKeyData **keys_out)
{
	ScanKeyData *keys;
	int			nkeys = 0;
	int			max_keys;
	ListCell   *lc;

	*keys_out = NULL;

	if (quals == NIL)
		return 0;

	max_keys = list_length(quals);
	keys = palloc(sizeof(ScanKeyData) * max_keys);

	foreach(lc, quals)
	{
		Node	   *qual = (Node *) lfirst(lc);
		OpExpr	   *opexpr;
		Var		   *var;
		Const	   *constval;
		Oid			opclass;
		Oid			opfamily;
		int			strategy;
		bool		var_on_left;

		/*
		 * Handle ScalarArrayOpExpr: "Var op ANY(ARRAY[...])" where the
		 * operator is an equality check.  We emit a single ScanKey with
		 * SK_SEARCHARRAY so the table AM can match against any element.
		 */
		if (IsA(qual, ScalarArrayOpExpr))
		{
			ScalarArrayOpExpr *saop = (ScalarArrayOpExpr *) qual;

			/* Only handle ANY (useOr), not ALL */
			if (!saop->useOr)
				continue;
			if (list_length(saop->args) != 2)
				continue;
			if (!IsA(linitial(saop->args), Var) ||
				!IsA(lsecond(saop->args), Const))
				continue;

			var = (Var *) linitial(saop->args);
			constval = (Const *) lsecond(saop->args);

			if (var->varno != scanrelid)
				continue;
			if (constval->constisnull)
				continue;

			/*
			 * Only same-type elements: a cross-type IN-list (e.g. datecol =
			 * ANY(timestamp[])) would be compared by the column type's own
			 * comparator against differently-typed datums.  The clause stays
			 * in the residual qual.
			 */
			if (get_base_element_type(constval->consttype) != var->vartype)
				continue;

			/*
			 * Only the column's own collation: comparisons run under
			 * attcollation, so a qual with an explicit COLLATE override must
			 * not be pushed.
			 */
			if (OidIsValid(saop->inputcollid) &&
				saop->inputcollid != var->varcollid)
				continue;

			/* Must be an equality operator */
			opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
			if (!OidIsValid(opclass))
				continue;
			opfamily = get_opclass_family(opclass);
			strategy = get_op_opfamily_strategy(saop->opno, opfamily);
			if (strategy != BTEqualStrategyNumber)
				continue;

			/* May need to grow the keys array */
			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = repalloc(keys, sizeof(ScanKeyData) * max_keys);
			}

			memset(&keys[nkeys], 0, sizeof(ScanKeyData));
			keys[nkeys].sk_flags = SK_SEARCHARRAY;
			keys[nkeys].sk_attno = var->varattno;
			keys[nkeys].sk_strategy = BTEqualStrategyNumber;
			keys[nkeys].sk_subtype = InvalidOid;
			keys[nkeys].sk_collation = saop->inputcollid;
			keys[nkeys].sk_argument = constval->constvalue;

			nkeys++;
			continue;
		}

		/*
		 * Handle NullTest: "Var IS NULL" or "Var IS NOT NULL".  These use
		 * SK_SEARCHNULL/SK_SEARCHNOTNULL flags with a NULL argument,
		 * mirroring how btree index scans represent these conditions.
		 */
		if (IsA(qual, NullTest))
		{
			NullTest   *ntest = (NullTest *) qual;
			Var		   *ntvar;

			if (!IsA(ntest->arg, Var))
				continue;

			/*
			 * ROW(...) IS NULL has per-field semantics that a storage
			 * nullness test cannot answer; leave it to the residual qual.
			 */
			if (ntest->argisrow)
				continue;

			ntvar = (Var *) ntest->arg;
			if (ntvar->varno != scanrelid)
				continue;

			if (nkeys >= max_keys)
			{
				max_keys *= 2;
				keys = repalloc(keys, sizeof(ScanKeyData) * max_keys);
			}

			memset(&keys[nkeys], 0, sizeof(ScanKeyData));
			keys[nkeys].sk_flags = (ntest->nulltesttype == IS_NULL) ?
				SK_SEARCHNULL : SK_SEARCHNOTNULL;
			keys[nkeys].sk_attno = ntvar->varattno;
			keys[nkeys].sk_strategy = InvalidStrategy;
			keys[nkeys].sk_subtype = InvalidOid;
			keys[nkeys].sk_collation = InvalidOid;
			keys[nkeys].sk_argument = (Datum) 0;

			nkeys++;
			continue;
		}

		if (!IsA(qual, OpExpr))
			continue;

		opexpr = (OpExpr *) qual;
		if (list_length(opexpr->args) != 2)
			continue;

		/* Check for (Var op Const) or (Const op Var) */
		if (IsA(linitial(opexpr->args), Var) &&
			IsA(lsecond(opexpr->args), Const))
		{
			var = (Var *) linitial(opexpr->args);
			constval = (Const *) lsecond(opexpr->args);
			var_on_left = true;
		}
		else if (IsA(linitial(opexpr->args), Const) &&
				 IsA(lsecond(opexpr->args), Var))
		{
			constval = (Const *) linitial(opexpr->args);
			var = (Var *) lsecond(opexpr->args);
			var_on_left = false;
		}
		else
			continue;

		/* Must reference this scan relation */
		if (var->varno != scanrelid)
			continue;

		/* Skip NULL constants */
		if (constval->constisnull)
			continue;

		/* Only the column's own collation; see the IN-list case above. */
		if (OidIsValid(opexpr->inputcollid) &&
			opexpr->inputcollid != var->varcollid)
			continue;

		/*
		 * Look up the btree strategy for this operator.  We need the default
		 * btree opclass for the variable's type.
		 */
		opclass = GetDefaultOpClass(var->vartype, BTREE_AM_OID);
		if (!OidIsValid(opclass))
			continue;

		opfamily = get_opclass_family(opclass);
		strategy = get_op_opfamily_strategy(opexpr->opno, opfamily);
		if (strategy == 0)
			continue;

		/*
		 * If Const is on the left, commute the strategy so it reads as
		 * "column op value".
		 */
		if (!var_on_left)
		{
			switch (strategy)
			{
				case BTLessStrategyNumber:
					strategy = BTGreaterStrategyNumber;
					break;
				case BTLessEqualStrategyNumber:
					strategy = BTGreaterEqualStrategyNumber;
					break;
				case BTGreaterEqualStrategyNumber:
					strategy = BTLessEqualStrategyNumber;
					break;
				case BTGreaterStrategyNumber:
					strategy = BTLessStrategyNumber;
					break;
				case BTEqualStrategyNumber:
					break;
				default:
					continue;
			}
		}

		/*
		 * Fill the ScanKeyData manually rather than using ScanKeyInit(),
		 * because we don't need sk_func populated — the table AM will use
		 * its own comparison functions.
		 */
		memset(&keys[nkeys], 0, sizeof(ScanKeyData));
		keys[nkeys].sk_flags = 0;
		keys[nkeys].sk_attno = var->varattno;
		keys[nkeys].sk_strategy = strategy;
		keys[nkeys].sk_subtype = (constval->consttype != var->vartype) ?
			constval->consttype : InvalidOid;
		keys[nkeys].sk_collation = opexpr->inputcollid;
		keys[nkeys].sk_argument = constval->constvalue;

		nkeys++;
	}

	if (nkeys == 0)
	{
		pfree(keys);
		return 0;
	}

	*keys_out = keys;
	return nkeys;
}
