DROP TABLE IF EXISTS test;

CREATE TABLE test(values Array(Nullable(Float64))) ENGINE=Memory;

SELECT '2 rows:';
INSERT INTO test VALUES ([1, NULL, 15, NULL, NULL, NULL]);
INSERT INTO test VALUES ([NULL, 7, NULL, 8, NULL, NULL]);
SELECT timeSeriesCoalesceGridValues('any')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values) AS result FROM test;

SELECT '3 rows:';
INSERT INTO test VALUES ([NULL, NULL, NULL, NULL, NULL, 4]);
SELECT timeSeriesCoalesceGridValues('any')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values) AS result FROM test;

SELECT '4 rows:';
INSERT INTO test VALUES ([NULL, NULL, 15, NULL, NULL, 4]);
SELECT timeSeriesCoalesceGridValues('any')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values) AS result FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values) AS result FROM test; -- {serverError CANNOT_EXECUTE_PROMQL_QUERY}

DROP TABLE test;

CREATE TABLE test(values Array(Nullable(Float64)), tags Array(Tuple(String, String))) ENGINE=Memory;

SELECT '1 row with group:';
INSERT INTO test VALUES ([1, NULL, NULL], [('__name__', 'up')]);
SELECT timeSeriesCoalesceGridValues('any')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values, timeSeriesTagsToGroup(tags)) FROM test;

SELECT '2 rows with group:';
INSERT INTO test VALUES ([NULL, 2, NULL], [('__name__', 'up')]);
SELECT timeSeriesCoalesceGridValues('any')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values, timeSeriesTagsToGroup(tags)) FROM test;

SELECT '3 rows with group:';
INSERT INTO test VALUES ([NULL, 2, 3], [('__name__', 'up')]);
SELECT timeSeriesCoalesceGridValues('any')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('nan')(values, timeSeriesTagsToGroup(tags)) FROM test;
SELECT timeSeriesCoalesceGridValues('throw')(values, timeSeriesTagsToGroup(tags)) FROM test; -- {serverError CANNOT_EXECUTE_PROMQL_QUERY}

DROP TABLE test;
