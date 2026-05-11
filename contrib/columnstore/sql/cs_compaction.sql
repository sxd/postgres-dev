--
-- Columnstore row group compaction tests (delta-to-columnar only)
--
-- Columnar DELETE and index builds land in later commits, so this stage
-- only exercises the parts of compaction that drive delta-store rows into
-- columnar row groups.
--

-- ===================================================================
-- Basic compaction: VACUUM moves delta rows into columnar storage
-- ===================================================================
CREATE TABLE cs_compact (id int, val text, num numeric(10,2)) USING columnstore;
INSERT INTO cs_compact SELECT i, 'row_' || i, (i * 1.5)::numeric(10,2)
    FROM generate_series(1, 500) i;
VACUUM cs_compact;

SELECT count(*) AS after_compact FROM cs_compact;
SELECT min(id), max(id) FROM cs_compact;
SELECT id, val, num FROM cs_compact WHERE id = 401;
SELECT id, val, num FROM cs_compact WHERE id = 500;

DROP TABLE cs_compact;

-- ===================================================================
-- Compaction with multiple row groups
-- ===================================================================
CREATE TABLE cs_compact_multi (id int, val text) USING columnstore;
-- Create 3 row groups via successive insert + vacuum
INSERT INTO cs_compact_multi SELECT i, 'rg1_' || i FROM generate_series(1, 300) i;
VACUUM cs_compact_multi;
INSERT INTO cs_compact_multi SELECT i, 'rg2_' || i FROM generate_series(301, 600) i;
VACUUM cs_compact_multi;
INSERT INTO cs_compact_multi SELECT i, 'rg3_' || i FROM generate_series(601, 900) i;
VACUUM cs_compact_multi;

SELECT count(*) AS multi_rg_count FROM cs_compact_multi;
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 1 AND 300;
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 301 AND 600;
SELECT count(*) FROM cs_compact_multi WHERE id BETWEEN 601 AND 900;

DROP TABLE cs_compact_multi;

-- ===================================================================
-- Compaction preserves data types correctly
-- ===================================================================
CREATE TABLE cs_compact_types (
    id int,
    i2 smallint,
    i8 bigint,
    t text,
    n numeric(10,2),
    d date,
    b bool
) USING columnstore;
INSERT INTO cs_compact_types SELECT
    i, (i % 100)::smallint, i::bigint * 1000000,
    'text_' || i, (i * 0.5)::numeric(10,2),
    '2020-01-01'::date + i, (i % 2 = 0)
    FROM generate_series(1, 500) i;

-- Save expected results
CREATE TABLE cs_compact_expected AS
    SELECT * FROM cs_compact_types;

VACUUM cs_compact_types;

-- Verify all types survived compaction
SELECT count(*) FROM (
    SELECT * FROM cs_compact_types
    EXCEPT
    SELECT * FROM cs_compact_expected
) diff;

DROP TABLE cs_compact_types;
DROP TABLE cs_compact_expected;

-- ===================================================================
-- Delta page reclamation: freed delta pages go onto the free list
-- so the relation doesn't grow indefinitely with insert/vacuum cycles
-- ===================================================================
CREATE TABLE cs_delta_reclaim (id int, val text) USING columnstore;

-- Cycle 1: insert and compact
INSERT INTO cs_delta_reclaim SELECT i, 'cycle1_' || i FROM generate_series(1, 500) i;
VACUUM cs_delta_reclaim;
SELECT pg_relation_size('cs_delta_reclaim') AS size_after_cycle1
    \gset

-- Cycle 2: insert more rows and compact again
INSERT INTO cs_delta_reclaim SELECT i, 'cycle2_' || i FROM generate_series(501, 1000) i;
VACUUM cs_delta_reclaim;
SELECT pg_relation_size('cs_delta_reclaim') AS size_after_cycle2
    \gset

-- Cycle 3: insert more rows and compact again
INSERT INTO cs_delta_reclaim SELECT i, 'cycle3_' || i FROM generate_series(1001, 1500) i;
VACUUM cs_delta_reclaim;

-- Relation size after cycle 3 should not be much larger than after cycle 2,
-- since the old delta pages should be reclaimed by the free list.
-- Without reclamation each cycle adds ~8 delta pages that are never freed.
SELECT pg_relation_size('cs_delta_reclaim') <=
       :'size_after_cycle2'::bigint * 2 AS delta_pages_reclaimed;

-- Data integrity: all 1500 rows should survive
SELECT count(*) AS total_rows FROM cs_delta_reclaim;

DROP TABLE cs_delta_reclaim;

-- ===================================================================
-- VACUUM updates pg_class.reltuples and relallvisible
-- ===================================================================
CREATE TABLE cs_relstats (id int, val text) USING columnstore;
INSERT INTO cs_relstats SELECT i, 'row_' || i FROM generate_series(1, 500) i;

-- Before VACUUM, reltuples may be 0 or stale
VACUUM cs_relstats;

-- After VACUUM (which compacts delta to columnar), reltuples should
-- reflect the actual row count
SELECT reltuples > 0 AS has_reltuples FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

-- relallvisible should be 0 (columnstore does not track page visibility)
SELECT relallvisible = 0 AS allvis_zero FROM pg_class
    WHERE oid = 'cs_relstats'::regclass;

DROP TABLE cs_relstats;

-- ===================================================================
-- VACUUM reports stats to pg_stat_user_tables
-- ===================================================================
CREATE TABLE cs_vacstats (id int, val text) USING columnstore;
INSERT INTO cs_vacstats SELECT i, 'row_' || i FROM generate_series(1, 500) i;
VACUUM cs_vacstats;

-- last_vacuum should be set and n_live_tup should reflect row count
SELECT last_vacuum IS NOT NULL AS has_last_vacuum,
       n_live_tup > 0 AS has_live_tup
    FROM pg_stat_user_tables WHERE relname = 'cs_vacstats';

DROP TABLE cs_vacstats;

-- ===================================================================
-- Freezing and relfrozenxid
-- ===================================================================
CREATE PROCEDURE cs_burn_xids(n int) LANGUAGE plpgsql AS $$
BEGIN
    FOR i IN 1..n LOOP
        PERFORM txid_current();
        COMMIT;
    END LOOP;
END $$;
-- relfrozenxid must advance across VACUUM (finding: pinned-at-creation
-- relfrozenxid eventually stalls the whole cluster at the wraparound
-- limit).  Burn xids so the pin would show as a large age, then check
-- that VACUUM's freeze pass plus an honest report brings the age back
-- close to the current xid.
CREATE TABLE cs_frozen (id int, val text) USING columnstore;
INSERT INTO cs_frozen SELECT i, 'f' || i FROM generate_series(1, 100) i;
CALL cs_burn_xids(5000);
SELECT age(relfrozenxid) > 4000 AS needs_freezing
  FROM pg_class WHERE relname = 'cs_frozen';
VACUUM cs_frozen;
SELECT age(relfrozenxid) < 1000 AS frozen_advanced
  FROM pg_class WHERE relname = 'cs_frozen';
-- rows survive freezing, in both storage forms
SELECT count(*), min(id), max(id) FROM cs_frozen;
INSERT INTO cs_frozen SELECT i, 'g' || i FROM generate_series(101, 150) i;
CALL cs_burn_xids(5000);
VACUUM cs_frozen;
SELECT age(relfrozenxid) < 1000 AS frozen_advanced_again
  FROM pg_class WHERE relname = 'cs_frozen';
SELECT count(*), min(id), max(id) FROM cs_frozen;
DROP TABLE cs_frozen;
DROP PROCEDURE cs_burn_xids(int);

-- compaction over delta pages too full to take even a minimal tuple:
-- ~2KB rows leave page remainders below the fence-worthiness threshold,
-- so the consumable prefix is processed without fencing those pages
CREATE TABLE cs_full_pages (id int, pad text) USING columnstore;
ALTER TABLE cs_full_pages ALTER COLUMN pad SET STORAGE PLAIN;
INSERT INTO cs_full_pages
    SELECT g, rpad(md5(g::text), 1996, 'x') FROM generate_series(1, 400) g;
VACUUM cs_full_pages;
SELECT count(*), min(id), max(id), max(length(pad)) FROM cs_full_pages;
-- the table keeps working for inserts and re-compaction afterwards
INSERT INTO cs_full_pages
    SELECT g, rpad(md5(g::text), 1996, 'x') FROM generate_series(401, 500) g;
VACUUM cs_full_pages;
SELECT count(*), max(id) FROM cs_full_pages;
DROP TABLE cs_full_pages;

-- Dropped columns: compaction must skip attisdropped attributes (their
-- atttypid is 0, so any type/opclass lookup would fail) and keep
-- compacting; subsequent DDL still works
CREATE TABLE cs_dropcol (a int, b text, c numeric(8,2)) USING columnstore;
INSERT INTO cs_dropcol SELECT g, 't' || g, g * 1.5 FROM generate_series(1, 500) g;
ALTER TABLE cs_dropcol DROP COLUMN b;
VACUUM cs_dropcol;
SELECT count(*), sum(a), sum(c) FROM cs_dropcol;
INSERT INTO cs_dropcol SELECT g, g * 2.5 FROM generate_series(501, 600) g;
VACUUM cs_dropcol;
ALTER TABLE cs_dropcol DROP COLUMN c;
VACUUM cs_dropcol;
SELECT count(*), sum(a) FROM cs_dropcol;
ALTER TABLE cs_dropcol ADD COLUMN d int DEFAULT 7;
SELECT count(*) FILTER (WHERE d = 7) AS defaults FROM cs_dropcol;
DROP TABLE cs_dropcol;

-- Zero-column rows through compaction
CREATE TABLE cs_zerocol_vac () USING columnstore;
INSERT INTO cs_zerocol_vac SELECT FROM generate_series(1, 200);
VACUUM cs_zerocol_vac;
SELECT count(*) FROM cs_zerocol_vac;
DELETE FROM cs_zerocol_vac;
VACUUM cs_zerocol_vac;
SELECT count(*) FROM cs_zerocol_vac;
DROP TABLE cs_zerocol_vac;

-- Subtransaction xids in delta tuples: released and rolled-back
-- savepoints (inserts and deletes) must classify correctly through
-- compaction
CREATE TABLE cs_subxact (v int) USING columnstore;
INSERT INTO cs_subxact SELECT g FROM generate_series(1, 500) g;
VACUUM cs_subxact;
BEGIN;
SAVEPOINT s1;
DELETE FROM cs_subxact WHERE v <= 100;
RELEASE s1;
SAVEPOINT s2;
DELETE FROM cs_subxact WHERE v > 400;
ROLLBACK TO s2;
INSERT INTO cs_subxact VALUES (1001);
SAVEPOINT s3;
INSERT INTO cs_subxact VALUES (1002);
ROLLBACK TO s3;
COMMIT;
SELECT count(*), min(v), max(v) FROM cs_subxact;
VACUUM cs_subxact;
SELECT count(*), min(v), max(v) FROM cs_subxact;
DROP TABLE cs_subxact;

-- A row group whose rows are all deleted, then compaction again, then
-- reuse of the table
CREATE TABLE cs_alldel (v int) USING columnstore;
INSERT INTO cs_alldel SELECT g FROM generate_series(1, 1000) g;
VACUUM cs_alldel;
DELETE FROM cs_alldel;
SELECT count(*) FROM cs_alldel;
VACUUM cs_alldel;
SELECT count(*) FROM cs_alldel;
INSERT INTO cs_alldel VALUES (42);
VACUUM cs_alldel;
SELECT * FROM cs_alldel;
DROP TABLE cs_alldel;

-- Wide table: the row group descriptor (per-column chunk descriptors + zone
-- maps) exceeds one catalog page beyond ~68 columns, so it is stored spanning
-- several consecutive pages.  DELETE + VACUUM materializes the tombstones into
-- the deletion bitmap and rewrites that multi-page descriptor; the rewrite must
-- update every page, not just the first.  Read columns from across the entry
-- (first, middle, last) after the rewrite to confirm it stayed intact.
SELECT format('CREATE TABLE cs_wide (%s) USING columnstore',
              string_agg(format('c%s int', g), ', '))
  FROM generate_series(1, 100) g \gexec
SELECT format('INSERT INTO cs_wide SELECT %s FROM generate_series(1, 2000) g',
              string_agg('g', ', '))
  FROM generate_series(1, 100) g \gexec
VACUUM cs_wide;
SELECT count(*), sum(c1), sum(c50), sum(c100) FROM cs_wide;
DELETE FROM cs_wide WHERE c1 % 7 = 0;
VACUUM cs_wide;
SELECT count(*), sum(c1), sum(c50), sum(c100) FROM cs_wide;
SELECT c1, c50, c100 FROM cs_wide WHERE c1 = 100;
DROP TABLE cs_wide;
