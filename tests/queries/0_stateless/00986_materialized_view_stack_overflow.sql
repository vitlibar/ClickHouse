DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS mv2;
DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;

CREATE TABLE test1 (a UInt8) ENGINE MergeTree ORDER BY a;
CREATE TABLE test2 (a UInt8) ENGINE MergeTree ORDER BY a;

CREATE MATERIALIZED VIEW mv1 TO test1 AS SELECT a FROM test2;
CREATE MATERIALIZED VIEW mv2 TO test2 AS SELECT a FROM test1;

insert into test1 values (1); -- { serverError 306 }

DROP TABLE mv1;
DROP TABLE mv2;
DROP TABLE test1;
DROP TABLE test2;
