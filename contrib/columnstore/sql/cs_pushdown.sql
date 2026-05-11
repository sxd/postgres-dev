--
-- Columnstore pushdown tests: scan key pushdown, bloom filter, ON CONFLICT
--

-- ===================================================================
-- IS NULL / IS NOT NULL scan key pushdown with zone map pruning
-- ===================================================================

-- Row group 1: all NULLs in v
CREATE TABLE cs_null_push (id int, v int) USING columnstore;
INSERT INTO cs_null_push SELECT i, NULL FROM generate_series(1, 500) i;
VACUUM cs_null_push;

-- Row group 2: no NULLs in v
INSERT INTO cs_null_push SELECT i, i FROM generate_series(501, 1000) i;
VACUUM cs_null_push;

-- IS NULL: should find only RG1's 500 rows
SELECT count(*) AS null_count FROM cs_null_push WHERE v IS NULL;

-- IS NOT NULL: should find only RG2's 500 rows
SELECT count(*) AS notnull_count FROM cs_null_push WHERE v IS NOT NULL;

-- Combined with value filter
SELECT count(*) FROM cs_null_push WHERE v IS NOT NULL AND v > 900;
SELECT count(*) FROM cs_null_push WHERE v IS NULL AND id < 100;

-- Row group 3: mixed NULLs and non-NULLs
INSERT INTO cs_null_push
    SELECT i, CASE WHEN i % 3 = 0 THEN NULL ELSE i END
    FROM generate_series(1001, 1500) i;
VACUUM cs_null_push;

SELECT count(*) AS null_count FROM cs_null_push WHERE v IS NULL;
SELECT count(*) AS notnull_count FROM cs_null_push WHERE v IS NOT NULL;

DROP TABLE cs_null_push;

-- ===================================================================
-- ScalarArrayOpExpr (IN-list) scan key pushdown
-- ===================================================================
CREATE TABLE cs_in_push (id int, cat text, val int) USING columnstore;
INSERT INTO cs_in_push SELECT i, 'cat_' || (i % 50), i
    FROM generate_series(1, 2000) i;
VACUUM cs_in_push;

-- IN-list on integer column — zone map should prune row groups
SELECT count(*) FROM cs_in_push WHERE val IN (1, 100, 500, 1000, 2000);

-- IN-list on dictionary-encoded text column
SELECT count(*) FROM cs_in_push WHERE cat IN ('cat_0', 'cat_1', 'cat_49');

-- IN-list combined with range filter
SELECT count(*) FROM cs_in_push WHERE val IN (1, 100, 200) AND id < 500;

-- Verify correctness against heap
CREATE TABLE cs_in_heap (id int, cat text, val int);
INSERT INTO cs_in_heap SELECT * FROM cs_in_push;

SELECT (SELECT count(*) FROM cs_in_push WHERE val IN (10, 20, 30)) =
       (SELECT count(*) FROM cs_in_heap WHERE val IN (10, 20, 30))
    AS in_matches;

SELECT (SELECT count(*) FROM cs_in_push WHERE cat IN ('cat_5', 'cat_10')) =
       (SELECT count(*) FROM cs_in_heap WHERE cat IN ('cat_5', 'cat_10'))
    AS in_text_matches;

DROP TABLE cs_in_push;
DROP TABLE cs_in_heap;

-- ===================================================================
-- Bloom filter pushdown from hash join
-- ===================================================================
CREATE TABLE cs_bloom_fact (id int, fk int, val text) USING columnstore;
INSERT INTO cs_bloom_fact SELECT i, i % 1000, 'row_' || i
    FROM generate_series(1, 5000) i;
VACUUM cs_bloom_fact;

-- Small dimension table (heap) — hash join should push bloom filter
CREATE TABLE cs_bloom_dim (id int PRIMARY KEY, label text);
INSERT INTO cs_bloom_dim SELECT i, 'dim_' || i FROM generate_series(1, 10) i;
ANALYZE cs_bloom_dim;
ANALYZE cs_bloom_fact;

-- Force hash join
SET enable_nestloop = off;
SET enable_mergejoin = off;

-- The bloom filter should substantially reduce rows scanned from cs_bloom_fact
-- (only 50 of 5000 rows match fk IN 1..10)
SELECT count(*) FROM cs_bloom_fact f JOIN cs_bloom_dim d ON f.fk = d.id;

-- With additional filter on the fact table
SELECT count(*) FROM cs_bloom_fact f JOIN cs_bloom_dim d ON f.fk = d.id
    WHERE f.id < 1000;

RESET enable_nestloop;
RESET enable_mergejoin;

-- Bloom filter with text (by-reference) join key
CREATE TABLE cs_bloom_txtfact (id int, cat text, val int) USING columnstore;
INSERT INTO cs_bloom_txtfact SELECT i, 'cat_' || (i % 100), i
    FROM generate_series(1, 3000) i;
VACUUM cs_bloom_txtfact;

CREATE TABLE cs_bloom_txtdim (cat text PRIMARY KEY);
INSERT INTO cs_bloom_txtdim VALUES ('cat_0'), ('cat_1'), ('cat_2');
ANALYZE cs_bloom_txtdim;
ANALYZE cs_bloom_txtfact;

SET enable_nestloop = off;
SET enable_mergejoin = off;

SELECT count(*) FROM cs_bloom_txtfact f JOIN cs_bloom_txtdim d ON f.cat = d.cat;

RESET enable_nestloop;
RESET enable_mergejoin;

DROP TABLE cs_bloom_fact;
DROP TABLE cs_bloom_dim;
DROP TABLE cs_bloom_txtfact;
DROP TABLE cs_bloom_txtdim;

-- ===================================================================
-- Multi-key bloom filter (composite join keys)
-- ===================================================================
CREATE TABLE cs_bloom_multi (a int, b int, val text) USING columnstore;
INSERT INTO cs_bloom_multi SELECT i % 100, i % 50, 'v_' || i
    FROM generate_series(1, 3000) i;
VACUUM cs_bloom_multi;

CREATE TABLE cs_bloom_multi_dim (a int, b int, label text);
INSERT INTO cs_bloom_multi_dim VALUES (1, 1, 'match1'), (2, 2, 'match2');
ANALYZE cs_bloom_multi_dim;
ANALYZE cs_bloom_multi;

SET enable_nestloop = off;
SET enable_mergejoin = off;

SELECT count(*) FROM cs_bloom_multi f
    JOIN cs_bloom_multi_dim d ON f.a = d.a AND f.b = d.b;

-- Verify correctness
SELECT (SELECT count(*) FROM cs_bloom_multi WHERE a = 1 AND b = 1) +
       (SELECT count(*) FROM cs_bloom_multi WHERE a = 2 AND b = 2)
    AS expected;

RESET enable_nestloop;
RESET enable_mergejoin;

DROP TABLE cs_bloom_multi;
DROP TABLE cs_bloom_multi_dim;

-- ===================================================================
-- ON CONFLICT (speculative insertion)
-- ===================================================================
CREATE TABLE cs_conflict (id int PRIMARY KEY, val text) USING columnstore;
INSERT INTO cs_conflict VALUES (1, 'first'), (2, 'second');

-- DO UPDATE: should update val for id=1
INSERT INTO cs_conflict VALUES (1, 'conflict')
    ON CONFLICT (id) DO UPDATE SET val = 'updated';
SELECT * FROM cs_conflict WHERE id = 1;

-- DO NOTHING: should silently skip
INSERT INTO cs_conflict VALUES (2, 'ignored')
    ON CONFLICT (id) DO NOTHING;
SELECT * FROM cs_conflict WHERE id = 2;

-- Non-conflicting insert should work normally
INSERT INTO cs_conflict VALUES (3, 'third')
    ON CONFLICT (id) DO NOTHING;
SELECT * FROM cs_conflict WHERE id = 3;

-- Batch ON CONFLICT
INSERT INTO cs_conflict VALUES (1, 'batch1'), (2, 'batch2'), (4, 'new4')
    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SELECT * FROM cs_conflict ORDER BY id;

-- ON CONFLICT with columnar rows (freeze first, then conflict)
VACUUM cs_conflict;
INSERT INTO cs_conflict VALUES (1, 'post_freeze')
    ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
SELECT * FROM cs_conflict WHERE id = 1;

DROP TABLE cs_conflict;

-- ON CONFLICT with expression index
CREATE TABLE cs_conflict_expr (id int, val text, lowval text GENERATED ALWAYS AS (lower(val)) STORED) USING columnstore;
CREATE UNIQUE INDEX ON cs_conflict_expr (lowval);
INSERT INTO cs_conflict_expr (id, val) VALUES (1, 'Hello');
INSERT INTO cs_conflict_expr (id, val) VALUES (2, 'HELLO')
    ON CONFLICT (lowval) DO UPDATE SET val = EXCLUDED.val;
SELECT id, val FROM cs_conflict_expr ORDER BY id;

DROP TABLE cs_conflict_expr;
