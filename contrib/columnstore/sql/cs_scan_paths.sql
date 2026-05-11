--
-- Columnstore scan path tests: point reads, column projection, COUNT(*) fast path
--

-- ===================================================================
-- Column projection on wide table
-- ===================================================================
CREATE TABLE cs_proj (
    c1 int, c2 int, c3 int, c4 int, c5 int,
    c6 int, c7 int, c8 int, c9 int, c10 int,
    c11 int, c12 int, c13 int, c14 int, c15 int,
    c16 int, c17 int, c18 int, c19 int, c20 int
) USING columnstore;
INSERT INTO cs_proj SELECT
    i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9,
    i+10, i+11, i+12, i+13, i+14, i+15, i+16, i+17, i+18, i+19
    FROM generate_series(1, 1000) i;
VACUUM cs_proj;

-- Access only first and last columns (should skip 18 in the middle)
SELECT c1, c20 FROM cs_proj WHERE c1 = 500;
-- Access only middle column
SELECT c10 FROM cs_proj WHERE c1 = 1;
-- Access all columns for one row
SELECT * FROM cs_proj WHERE c1 = 999;

DROP TABLE cs_proj;

-- ===================================================================
-- Point reads via index scan on FOR-encoded columns
-- ===================================================================
CREATE TABLE cs_point_for (id int, v int, padding text) USING columnstore;
INSERT INTO cs_point_for SELECT i, 1000 + (i % 100), 'pad_' || i
    FROM generate_series(1, 2000) i;
VACUUM cs_point_for;
CREATE INDEX ON cs_point_for (id);
ANALYZE cs_point_for;

SET enable_seqscan = off;
-- Single point read
SELECT id, v FROM cs_point_for WHERE id = 42;
SELECT id, v FROM cs_point_for WHERE id = 1999;
-- Range that stays within point-read threshold
SELECT count(*) FROM cs_point_for WHERE id BETWEEN 1 AND 50;
RESET enable_seqscan;
DROP TABLE cs_point_for;

-- ===================================================================
-- Point reads on NI64-encoded columns
-- ===================================================================
CREATE TABLE cs_point_ni64 (id int, v numeric(10,2)) USING columnstore;
INSERT INTO cs_point_ni64 SELECT i, (i * 1.23)::numeric(10,2)
    FROM generate_series(1, 2000) i;
VACUUM cs_point_ni64;
CREATE INDEX ON cs_point_ni64 (id);
ANALYZE cs_point_ni64;

SET enable_seqscan = off;
SELECT id, v FROM cs_point_ni64 WHERE id = 100;
SELECT id, v FROM cs_point_ni64 WHERE id = 1500;
-- Verify aggregate on index-scanned subset
SELECT sum(v) FROM cs_point_ni64 WHERE id BETWEEN 1 AND 10;
RESET enable_seqscan;
DROP TABLE cs_point_ni64;

-- ===================================================================
-- Sequential scan with filter on various encodings
-- ===================================================================
CREATE TABLE cs_scan_filter (
    id int,
    v_for int,      -- FOR-encoded
    v_ni64 numeric(10,2), -- NI64
    v_dict text,    -- dictionary
    v_plain text    -- plain
) USING columnstore;
INSERT INTO cs_scan_filter SELECT
    i,
    1000 + (i % 50),
    (i * 0.99)::numeric(10,2),
    'cat_' || (i % 20),
    'unique_' || i
    FROM generate_series(1, 2000) i;
VACUUM cs_scan_filter;

-- Filter on FOR column
SELECT count(*) FROM cs_scan_filter WHERE v_for = 1025;
-- Filter on NI64 column
SELECT count(*) FROM cs_scan_filter WHERE v_ni64 < 10.00;
-- Filter on dict column
SELECT count(*) FROM cs_scan_filter WHERE v_dict = 'cat_0';
-- Filter on plain column
SELECT id FROM cs_scan_filter WHERE v_plain = 'unique_1000';

DROP TABLE cs_scan_filter;

-- ===================================================================
-- COUNT(*) fast path correctness
-- ===================================================================

-- Pure columnar, no deletions
CREATE TABLE cs_cnt (id int) USING columnstore;
INSERT INTO cs_cnt SELECT i FROM generate_series(1, 2000) i;
VACUUM cs_cnt;
SELECT count(*) AS pure FROM cs_cnt;

-- After deletion
DELETE FROM cs_cnt WHERE id % 10 = 0;
SELECT count(*) AS after_del FROM cs_cnt;

-- Mixed delta + columnar
INSERT INTO cs_cnt SELECT i FROM generate_series(3000, 3100) i;
SELECT count(*) AS mixed FROM cs_cnt;

-- With filter (not pure COUNT(*) fast path)
SELECT count(*) AS filtered FROM cs_cnt WHERE id > 1000;

DROP TABLE cs_cnt;

-- ===================================================================
-- Scan with all-NULL column
-- ===================================================================
CREATE TABLE cs_scan_allnull (id int, v int) USING columnstore;
INSERT INTO cs_scan_allnull SELECT i, NULL FROM generate_series(1, 500) i;
VACUUM cs_scan_allnull;
SELECT count(*) FROM cs_scan_allnull;
SELECT count(v) FROM cs_scan_allnull;
SELECT count(*) FROM cs_scan_allnull WHERE v IS NULL;
SELECT count(*) FROM cs_scan_allnull WHERE v IS NOT NULL;
DROP TABLE cs_scan_allnull;
