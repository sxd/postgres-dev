--
-- Test for DDL statement from:
-- - pg_get_publication_ddl
--

-- suppress warning that depends on wal_level
SET client_min_messages = 'ERROR';

-- Run the body under a stable role so the ALTER PUBLICATION ... OWNER TO
-- output is deterministic across environments.
CREATE ROLE regress_publication_ddl_user LOGIN SUPERUSER;
SET SESSION AUTHORIZATION 'regress_publication_ddl_user';

-- test with a non-existing publication
SELECT pg_get_publication_ddl('non-existing');
SELECT pg_get_publication_ddl(0::oid);

-- empty publication is possible and allowed
CREATE PUBLICATION testpub_ddl_1;
SELECT pg_get_publication_ddl('testpub_ddl_1');

-- owner option: 'false' drops the trailing ALTER PUBLICATION ... OWNER TO.
-- 'true' keeps it; its output matches the default but it exercises the
-- option's explicit-true code path, so keep both.
SELECT pg_get_publication_ddl('testpub_ddl_1', 'owner', 'false');
SELECT pg_get_publication_ddl('testpub_ddl_1', 'owner', 'true');

-- NULL input should produce an empty result set
SELECT count(*) = 0 AS is_null FROM pg_get_publication_ddl(NULL::oid);
SELECT count(*) = 0 AS is_null FROM pg_get_publication_ddl(NULL::text);

-- invalid option arguments are rejected
SELECT pg_get_publication_ddl('testpub_ddl_1', 'pretty');           -- odd number of arguments
SELECT pg_get_publication_ddl('testpub_ddl_1', 'bogus', 'true');    -- unrecognized option
SELECT pg_get_publication_ddl('testpub_ddl_1', 'pretty', NULL);     -- null option value
SELECT pg_get_publication_ddl('testpub_ddl_1', NULL, 'true');       -- null option name

-- create set of tables for publications
CREATE TABLE testpub_ddl_tbl1 (foo int, bar int);
CREATE TABLE testpub_ddl_tbl2 (foo int, bar int);
CREATE TABLE testpub_ddl_tbl3 (foo int, bar int, beque int, baz int);
CREATE TABLE testpub_ddl_tbl4 (foo int, bar int, beque bool);
CREATE TABLE testpub_ddl_tbl5 (foo int, "bar beque" int);

CREATE PUBLICATION testpub_ddl_2 FOR TABLE testpub_ddl_tbl1, testpub_ddl_tbl2, testpub_ddl_tbl3 WITH (publish='delete', publish_generated_columns='stored', publish_via_partition_root='true');

SELECT pg_get_publication_ddl('testpub_ddl_2');
SELECT pg_get_publication_ddl((SELECT oid FROM pg_publication WHERE pubname='testpub_ddl_2'));
SELECT pg_get_publication_ddl('testpub_ddl_2', 'pretty', 'true');

ALTER PUBLICATION testpub_ddl_2 SET (publish = 'delete, update');

SELECT pg_get_publication_ddl('testpub_ddl_2');

-- publish list with only truncate (truncate as the first/only action)
CREATE PUBLICATION testpub_ddl_trunc FOR TABLE testpub_ddl_tbl1 WITH (publish='truncate');
SELECT pg_get_publication_ddl('testpub_ddl_trunc');

-- create publication for one table
CREATE PUBLICATION testpub_ddl_3 FOR TABLE ONLY testpub_ddl_tbl1;

SELECT pg_get_publication_ddl('testpub_ddl_3');

-- create publication for one table with two columns and a rowfilter
CREATE PUBLICATION testpub_ddl_4 FOR TABLE ONLY testpub_ddl_tbl3 (bar,baz) WHERE (bar = baz);

SELECT pg_get_publication_ddl('testpub_ddl_4');

-- create publication for all tables
CREATE PUBLICATION testpub_ddl_6 FOR ALL TABLES;

SELECT pg_get_publication_ddl('testpub_ddl_6');

-- create publication for all sequences
CREATE PUBLICATION testpub_ddl_7 FOR ALL SEQUENCES;
SELECT pg_get_publication_ddl('testpub_ddl_7');

-- create publication for all tables and all sequences
CREATE PUBLICATION testpub_ddl_8 FOR ALL TABLES, ALL SEQUENCES;

SELECT pg_get_publication_ddl('testpub_ddl_8');

-- create a publication with a bare bolean in the row filter
CREATE PUBLICATION testpub_ddl_10 FOR TABLE testpub_ddl_tbl4 WHERE (beque);
SELECT pg_get_publication_ddl('testpub_ddl_10');

-- create schema for schema publication
CREATE SCHEMA pub_schema_test_ddl;
CREATE TABLE pub_schema_test_ddl.schema_tbl1 (foo int, bar int);
CREATE TABLE pub_schema_test_ddl.schema_tbl2 (foo int, bar int);
CREATE TABLE pub_schema_test_ddl.schema_tbl3 (foo int, bar int, baz int);

-- create a publication for a list of tables and schema
CREATE PUBLICATION testpub_ddl_schema_1 FOR TABLE pub_schema_test_ddl.schema_tbl1, pub_schema_test_ddl.schema_tbl2, TABLES IN SCHEMA pub_schema_test_ddl;
SELECT pg_get_publication_ddl('testpub_ddl_schema_1');

-- create publication in schema only for table
CREATE PUBLICATION testpub_ddl_schema_2 FOR TABLES IN SCHEMA pub_schema_test_ddl, TABLE pub_schema_test_ddl.schema_tbl1;
SELECT pg_get_publication_ddl('testpub_ddl_schema_2');

-- create publication for all tables in schema
CREATE PUBLICATION testpub_ddl_schema_3 FOR TABLES IN SCHEMA pub_schema_test_ddl;
SELECT pg_get_publication_ddl('testpub_ddl_schema_3');

-- a new schema for multiple schemas
CREATE SCHEMA pub_schema_test_ddl_2;
CREATE TABLE pub_schema_test_ddl_2.schema_tbl1 (foo int, bar int);

-- create a publication for a list of schemas
CREATE PUBLICATION testpub_ddl_schema_4 FOR TABLES IN SCHEMA pub_schema_test_ddl, pub_schema_test_ddl_2;
SELECT pg_get_publication_ddl('testpub_ddl_schema_4');

-- create a publication for a specific schema and a table in public schema
-- both with the same name
CREATE TABLE schema_tbl1 (foo int, bar int);
CREATE PUBLICATION testpub_ddl_schema_5  FOR TABLE pub_schema_test_ddl.schema_tbl1, schema_tbl1;
SELECT pg_get_publication_ddl('testpub_ddl_schema_5');

-- identifiers that require quoting: publication, schema, table and column
CREATE SCHEMA "Pub Schema";
CREATE TABLE "Pub Schema"."Quoted Table" ("Col One" int, "select" int);
CREATE PUBLICATION "testpub Quoted Pub" FOR TABLE "Pub Schema"."Quoted Table" ("Col One", "select") WHERE ("Col One" > 0);
SELECT pg_get_publication_ddl('testpub Quoted Pub');

-- tables for EXCEPT
CREATE TABLE testpub_ddl_except1 (foo int, bar int);
CREATE TABLE testpub_ddl_except2 (foo int, bar int);

-- create publication for all tables except one
CREATE PUBLICATION testpub_ddl_except1 FOR ALL TABLES EXCEPT (TABLE testpub_ddl_except1);
SELECT pg_get_publication_ddl('testpub_ddl_except1');

-- create publication for all sequences and all tables except two tables
CREATE PUBLICATION testpub_ddl_except2 FOR ALL SEQUENCES, ALL TABLES EXCEPT (TABLE testpub_ddl_except1, testpub_ddl_except2);
SELECT pg_get_publication_ddl('testpub_ddl_except2');

-- get all the created publications DDL into the table
CREATE TEMP TABLE pub_ddl AS
SELECT p.pubname, t.n, t.stmt
FROM pg_publication p,
LATERAL pg_get_publication_ddl(p.pubname) WITH ORDINALITY AS t(stmt, n)
WHERE p.pubname LIKE 'testpub%';

-- drop the publications to be recreated
SELECT format('DROP PUBLICATION %I', pubname)
FROM (SELECT DISTINCT pubname FROM pub_ddl) ORDER BY pubname \gexec

-- recreate all the publications using the ddl from pg_get_publication_ddl()
SELECT stmt FROM pub_ddl ORDER BY pubname, n \gexec

-- cleanup publications
SELECT format('DROP PUBLICATION %I', pubname)
FROM (SELECT DISTINCT pubname FROM pub_ddl) ORDER BY pubname \gexec

-- cleanup tables
DROP TABLE testpub_ddl_tbl1;
DROP TABLE testpub_ddl_tbl2;
DROP TABLE testpub_ddl_tbl3;
DROP TABLE testpub_ddl_tbl4;
DROP TABLE testpub_ddl_tbl5;
DROP TABLE pub_ddl;

--- cleanup tables for schema tests
DROP TABLE schema_tbl1;

-- cleanup tables for quoted names
DROP TABLE "Pub Schema"."Quoted Table";

-- cleanup tables for except
DROP TABLE testpub_ddl_except1;
DROP TABLE testpub_ddl_except2;

-- cleanup schemas
DROP SCHEMA pub_schema_test_ddl CASCADE;
DROP SCHEMA pub_schema_test_ddl_2 CASCADE;
DROP SCHEMA "Pub Schema";

-- cleanup role
RESET SESSION AUTHORIZATION;
DROP ROLE regress_publication_ddl_user;

RESET client_min_messages;
