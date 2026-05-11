-- Tests for the columnstore table access method (delta-store stage)
--
-- At this commit the AM stores every row in the delta heap.  The
-- delta-to-columnar compaction path lands in a later commit, so the
-- tests here exercise CREATE / INSERT / COPY / SELECT / DELETE /
-- UPDATE / TRUNCATE without invoking VACUUM.

CREATE EXTENSION columnstore;

CREATE TABLE cs_basic (a int, b text) USING columnstore;

-- Columnstore-tuned autovacuum / fillfactor defaults are stamped on
-- the relation at create time.
SELECT relname, reloptions FROM pg_class WHERE relname = 'cs_basic';

INSERT INTO cs_basic SELECT g, 'val' || g FROM generate_series(1, 50) g;

SELECT count(*) FROM cs_basic;
SELECT a, b FROM cs_basic WHERE a = 25;
SELECT a FROM cs_basic ORDER BY a LIMIT 5;

-- DELETE from the delta store
DELETE FROM cs_basic WHERE a <= 5;
SELECT count(*) FROM cs_basic;

-- UPDATE in the delta store (delete + insert under the hood)
UPDATE cs_basic SET b = 'updated' WHERE a = 10;
SELECT a, b FROM cs_basic WHERE a = 10;

-- Transaction visibility
BEGIN;
INSERT INTO cs_basic VALUES (1000, 'rolled');
SELECT count(*) FROM cs_basic;
ROLLBACK;
SELECT count(*) FROM cs_basic;

-- TRUNCATE
TRUNCATE cs_basic;
SELECT count(*) FROM cs_basic;

DROP TABLE cs_basic;

-- INSERT ... SELECT between columnstore tables and to/from heap
CREATE TABLE cs_src (a int, b text) USING columnstore;
INSERT INTO cs_src SELECT g, 'src' || g FROM generate_series(1, 20) g;

CREATE TABLE cs_dst (a int, b text) USING columnstore;
INSERT INTO cs_dst SELECT a, b FROM cs_src;
SELECT count(*) FROM cs_dst;

CREATE TABLE cs_heap_dst (a int, b text);
INSERT INTO cs_heap_dst SELECT a, b FROM cs_src;
SELECT count(*) FROM cs_heap_dst;

DROP TABLE cs_dst;
DROP TABLE cs_heap_dst;
DROP TABLE cs_src;

-- COPY into delta
CREATE TABLE cs_copy (a int, b text) USING columnstore;
COPY cs_copy FROM stdin;
1	one
2	two
3	three
\.
SELECT count(*) FROM cs_copy ORDER BY 1;
SELECT * FROM cs_copy ORDER BY a;

DROP TABLE cs_copy;

-- Zero-column tables: a zero-attribute data tuple must not be mistaken
-- for a tombstone (tombstones are zero-attribute tuples distinguished by
-- their columnar-space t_ctid)
CREATE TABLE cs_zerocol () USING columnstore;
INSERT INTO cs_zerocol DEFAULT VALUES;
INSERT INTO cs_zerocol SELECT FROM generate_series(1, 50);
SELECT count(*) FROM cs_zerocol;
DELETE FROM cs_zerocol;
SELECT count(*) FROM cs_zerocol;
DROP TABLE cs_zerocol;

-- Partitioned parents may carry the columnstore AM: the autovacuum
-- defaults event trigger must skip them (no storage parameters on 'p'
-- relations), and partitions inherit the AM
CREATE TABLE cs_part (k int, v int) PARTITION BY RANGE (k) USING columnstore;
CREATE TABLE cs_part1 PARTITION OF cs_part FOR VALUES FROM (0) TO (100);
CREATE TABLE cs_part2 PARTITION OF cs_part FOR VALUES FROM (100) TO (200);
SELECT c.relname, a.amname,
       c.reloptions IS NOT NULL AS has_av_defaults
    FROM pg_class c LEFT JOIN pg_am a ON a.oid = c.relam
    WHERE c.relname IN ('cs_part', 'cs_part1', 'cs_part2')
    ORDER BY c.relname;
INSERT INTO cs_part SELECT g % 200, g FROM generate_series(1, 400) g;
SELECT count(*), sum(v) FROM cs_part;
SELECT count(*) FROM cs_part WHERE k < 100;
DELETE FROM cs_part WHERE v <= 200;
SELECT count(*), sum(v) FROM cs_part;
DROP TABLE cs_part;

-- The defaults are applied at creation, including CREATE TABLE AS /
-- SELECT INTO.  A table converted with ALTER TABLE ... SET ACCESS METHOD
-- columnstore does NOT get them (the trigger fires only on creation, so
-- that a later ALTER TABLE ... RESET of these options is not re-imposed).
CREATE TABLE cs_avd_ctas USING columnstore AS SELECT 1 AS v;
CREATE TABLE cs_avd_heap (v int);
ALTER TABLE cs_avd_heap SET ACCESS METHOD columnstore;
SELECT relname, reloptions IS NOT NULL AS has_av_defaults
    FROM pg_class
    WHERE relname IN ('cs_avd_ctas', 'cs_avd_heap')
    ORDER BY relname;
DROP TABLE cs_avd_ctas, cs_avd_heap;

-- The autovacuum defaults are a creation-time convenience, not a lock-in:
-- a user can override them, and RESET must fall back to the GUC default
-- rather than being silently re-imposed by the event trigger.
CREATE TABLE cs_avd_reset (v int) USING columnstore;
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
ALTER TABLE cs_avd_reset SET (autovacuum_vacuum_insert_threshold = 5000);
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
ALTER TABLE cs_avd_reset RESET (autovacuum_vacuum_insert_threshold);
-- an unrelated ALTER afterwards must not bring the default back
ALTER TABLE cs_avd_reset ADD COLUMN w int;
SELECT reloptions FROM pg_class WHERE relname = 'cs_avd_reset';
DROP TABLE cs_avd_reset;

-- An option the user supplies at CREATE time is kept; the trigger only
-- fills in the one that is missing (here it must not overwrite the
-- explicit threshold, but must still add the scale_factor default).
CREATE TABLE cs_avd_explicit (v int) USING columnstore
    WITH (autovacuum_vacuum_insert_threshold = 5000);
SELECT unnest(reloptions) AS opt FROM pg_class
    WHERE relname = 'cs_avd_explicit' ORDER BY opt;
DROP TABLE cs_avd_explicit;
