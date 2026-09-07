-- The `samples_partition_by` setting defines the partition key of the inner samples table;
-- without it the table is partitioned by month.

SET allow_experimental_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS ts_copy;
DROP TABLE IF EXISTS samples_ext;

SELECT '-- the default partition key makes one partition per month';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('m', map('env', 'prod'), [(toDateTime64('2026-01-15 10:00:00', 3), 1.), (toDateTime64('2026-02-15 10:00:00', 3), 2.), (toDateTime64('2026-02-20 10:00:00', 3), 3.)]);

SELECT partition, sum(rows) AS rows
FROM system.parts WHERE database = currentDatabase() AND table LIKE '.inner\_id.samples.%' AND active
GROUP BY partition ORDER BY partition;

SELECT metric_name, time_series FROM ts ORDER BY time_series;

DROP TABLE ts;

SELECT '-- the setting overrides the partition key of a declared engine';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toStartOfWeek(bucket)'
SAMPLES ENGINE = AggregatingMergeTree PARTITION BY toDate(bucket) ORDER BY (id, bucket);
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';
DROP TABLE ts;

SELECT '-- a declared partition key is kept if the setting is not set';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ENGINE = AggregatingMergeTree PARTITION BY toDate(bucket) ORDER BY (id, bucket);
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';
DROP TABLE ts;

SELECT '-- CREATE AS generates the partition key again for the new table';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
CREATE TABLE ts_copy AS ts ENGINE = TimeSeries SETTINGS samples_partition_by = 'toStartOfWeek(bucket)';
SELECT extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) TAGS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts;

SELECT '-- CREATE AS copies a customized partition key';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0
SAMPLES ENGINE = AggregatingMergeTree PARTITION BY toDate(bucket) ORDER BY (id, bucket);
CREATE TABLE ts_copy AS ts ENGINE = TimeSeries;
SELECT extract(create_table_query, 'SAMPLES INNER ENGINE = (.*?) TAGS INNER')
FROM system.tables WHERE database = currentDatabase() AND name = 'ts_copy';
DROP TABLE ts_copy;
DROP TABLE ts;

SELECT '-- the setting is ignored for an external samples table';

CREATE TABLE samples_ext
(
    `id` Tuple(UInt64, LowCardinality(UUID)),
    `samples` SimpleAggregateFunction(timeSeriesGroupArray, Array(Tuple(timestamp DateTime64(3), value Float64))),
    `bucket` DateTime64(3),
    `min_time` SimpleAggregateFunction(min, DateTime64(3)),
    `max_time` SimpleAggregateFunction(max, DateTime64(3))
)
ENGINE = AggregatingMergeTree ORDER BY (id, bucket);
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toStartOfWeek(bucket)' SAMPLES samples_ext;
SELECT engine, partition_key FROM system.tables WHERE database = currentDatabase() AND name = 'samples_ext';
DROP TABLE ts;
DROP TABLE samples_ext;

SELECT '-- the setting requires a MergeTree-family engine of the inner samples table';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, samples_partition_by = 'toStartOfWeek(bucket)'
SAMPLES ENGINE = Memory; -- { serverError INVALID_SETTING_VALUE }

SELECT '-- the setting cannot be changed after the table is created';

CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0;
ALTER TABLE ts MODIFY SETTING samples_partition_by = 'toStartOfWeek(bucket)'; -- { serverError NOT_IMPLEMENTED }
DROP TABLE ts;
