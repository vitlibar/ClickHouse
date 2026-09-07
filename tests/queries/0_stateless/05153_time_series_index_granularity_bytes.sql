-- Tags: no-parallel-replicas
-- Tag no-parallel-replicas: `engine_full` of the inner tables is read from `system.tables` on the initiator only.

SET allow_experimental_time_series_table = 1;

SELECT '-- default index granularity of the inner samples tables';
DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, extractAll(engine_full, '(index_granularity\w* = \d+)') AS granularity
FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner\_id.samples.%' OR name LIKE '.inner\_id.recentsamples.%')
ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- the settings override the defaults';
CREATE TABLE ts ENGINE = TimeSeries
SETTINGS samples_index_granularity = 256, samples_index_granularity_bytes = 4194304,
         recent_samples_index_granularity = 8192, recent_samples_index_granularity_bytes = 65536;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, extractAll(engine_full, '(index_granularity\w* = \d+)') AS granularity
FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner\_id.samples.%' OR name LIKE '.inner\_id.recentsamples.%')
ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- the values from the engine declaration are kept unless the settings are set explicitly';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_index_granularity_bytes = 131072
SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS index_granularity_bytes = 2097152
RECENT SAMPLES INNER ENGINE = AggregatingMergeTree SETTINGS index_granularity_bytes = 2097152;
SELECT extract(name, '^\.inner_id\.(\w+)\.') AS inner_table, extractAll(engine_full, '(index_granularity\w* = \d+)') AS granularity
FROM system.tables WHERE database = currentDatabase() AND (name LIKE '.inner\_id.samples.%' OR name LIKE '.inner\_id.recentsamples.%')
ORDER BY inner_table;
DROP TABLE ts;

SELECT '-- the settings are ignored for a non-MergeTree engine';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS samples_index_granularity_bytes = 131072, recent_samples_ttl_seconds = 0
SAMPLES INNER ENGINE = Memory;
SELECT engine_full FROM system.tables WHERE database = currentDatabase() AND name LIKE '.inner\_id.samples.%';
DROP TABLE ts;

SELECT '-- the setting of the recent samples table requires the table';
CREATE TABLE ts ENGINE = TimeSeries SETTINGS recent_samples_ttl_seconds = 0, recent_samples_index_granularity_bytes = 131072; -- { serverError INVALID_SETTING_VALUE }
