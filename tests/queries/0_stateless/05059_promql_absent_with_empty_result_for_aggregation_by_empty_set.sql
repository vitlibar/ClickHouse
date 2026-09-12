-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL grammar requires it.
-- The experimental TimeSeries table engine does not round-trip through DatabaseReplicated.

-- `absent` and `absent_over_time` count the present series with an aggregation without keys. Such an aggregation
-- over an empty input returns no rows when `empty_result_for_aggregation_by_empty_set` is enabled, but an empty
-- input is exactly the case when these functions must produce their synthetic series. The translator adds a neutral
-- row to the aggregated input, so the result must not depend on the setting.

SET allow_experimental_time_series_table = 1;
SET allow_experimental_time_series_aggregate_functions = 1;
SET empty_result_for_aggregation_by_empty_set = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 1.0), (toDateTime64(1699999940, 3), 1.0), (toDateTime64(1700000000, 3), 1.0)]);

SELECT 'absent: no series matches, the labels come from the equality matchers';
SELECT tags, value FROM prometheusQuery(ts, 'absent(up{instance="nohost"})', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent(nonexistent{job="api"})', 1700000000);

SELECT 'absent: a series is present, so the result is empty';
SELECT tags, value FROM prometheusQuery(ts, 'absent(up)', 1700000000);

SELECT 'absent_over_time: no series has a sample in the window';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(up{instance="nohost"}[5m])', 1700000000);
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(nonexistent{job="api"}[5m])', 1700000000);

SELECT 'absent_over_time: a series has samples in the window, so the result is empty';
SELECT tags, value FROM prometheusQuery(ts, 'absent_over_time(up[5m])', 1700000000);

-- The last sample is at 1700000000, so the series is present at the steps 1700000000 and 1700000200 (via the
-- 5-minute lookback and the 5-minute range respectively) and absent at the steps 1700000400 and 1700000600.
SELECT 'range query: the synthetic series appears only at the steps where the real series is gone';
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), time_series) AS time_series
FROM prometheusQueryRange(ts, 'absent(up)', 1700000000, 1700000600, 200);
SELECT tags, arrayMap(x -> (toUnixTimestamp64Second(x.1), x.2), time_series) AS time_series
FROM prometheusQueryRange(ts, 'absent_over_time(up[5m])', 1700000000, 1700000600, 200);

DROP TABLE ts;
