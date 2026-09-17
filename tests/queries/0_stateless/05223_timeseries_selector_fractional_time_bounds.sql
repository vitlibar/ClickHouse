-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
--
-- The arguments `min_time` and `max_time` of table function `timeSeriesSelector` can have a greater scale than the timestamps
-- in the TimeSeries table: the time range has at least millisecond precision, and it is converted to the scale of the table
-- with the bounds rounded towards the inside of the range. The returned columns keep the types of the table.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts_dt;
DROP TABLE IF EXISTS ts_u32;
DROP TABLE IF EXISTS ts_dt64_1;

CREATE TABLE ts_dt (samples Array(Tuple(DateTime('UTC'), Float64))) ENGINE = TimeSeries;
CREATE TABLE ts_u32 (samples Array(Tuple(UInt32, Float32))) ENGINE = TimeSeries;
CREATE TABLE ts_dt64_1 (samples Array(Tuple(DateTime64(1, 'UTC'), Float64))) ENGINE = TimeSeries;

INSERT INTO ts_dt (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime(1000, 'UTC'), 1), (toDateTime(1001, 'UTC'), 2), (toDateTime(1002, 'UTC'), 3), (toDateTime(1003, 'UTC'), 4)]);
INSERT INTO ts_u32 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(1000, 1), (1001, 2), (1002, 3), (1003, 4)]);
INSERT INTO ts_dt64_1 (metric_name, tags, samples) VALUES ('up', {'job': 'j'}, [(toDateTime64(1000, 1, 'UTC'), 1), (toDateTime64(1001.5, 1, 'UTC'), 2), (toDateTime64(1002, 1, 'UTC'), 3), (toDateTime64(1003, 1, 'UTC'), 4)]);

SELECT '-- The returned columns keep the types of the table';
DESCRIBE timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5);
DESCRIBE timeSeriesSelector(ts_u32, 'up', 1000.5, 1002.5);
DESCRIBE timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5);

SELECT '-- [1000.5, 1002.5] selects the samples at 1001 and 1002 (and 1001.5 in DateTime64(1))';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1000.5, 1002.5) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', 1000.5, 1002.5) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', 1000.5, 1002.5) ORDER BY timestamp;

SELECT '-- The bounds are inclusive at millisecond precision';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1001, 1002) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1000.999, 1002.001) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', 1001.001, 1001.999) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', 1001.001, 1001.999) ORDER BY timestamp;

SELECT '-- DateTime64 and String arguments';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1002.5, 3, 'UTC')) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt, 'up', '1970-01-01 00:16:40.5', '1970-01-01 00:16:42.5') ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_u32, 'up', '1000.5', '1002.5') ORDER BY timestamp;

SELECT '-- A scale greater than 3 in the arguments is kept';
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1001.49999, 5, 'UTC'), toDateTime64(1001.50001, 5, 'UTC')) ORDER BY timestamp;
SELECT timestamp, value FROM timeSeriesSelector(ts_dt64_1, 'up', toDateTime64(1001.50001, 5, 'UTC'), toDateTime64(1001.99999, 5, 'UTC')) ORDER BY timestamp;

SELECT '-- The range is checked before the conversion to the scale of the table';
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 1001.2, 1001.8);
SELECT count() FROM timeSeriesSelector(ts_dt, 'up', 1002.5, 1000.5); -- { serverError BAD_ARGUMENTS }

DROP TABLE ts_dt;
DROP TABLE ts_u32;
DROP TABLE ts_dt64_1;
