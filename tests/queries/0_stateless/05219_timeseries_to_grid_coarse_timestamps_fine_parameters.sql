-- The grid parameters of the timeSeries*ToGrid functions can have a greater scale than the timestamps of the samples:
-- the timestamps are converted to the scale of the parameters. So UInt32, DateTime and DateTime64(1) timestamps
-- must give the same results as the same samples stored as DateTime64(3).

SET allow_experimental_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts_coarse;

CREATE TABLE ts_coarse (ts_u32 UInt32, ts_dt DateTime('UTC'), ts_dt64_1 DateTime64(1, 'UTC'), ts_dt64_3 DateTime64(3, 'UTC'), value Float64)
ENGINE = MergeTree ORDER BY ts_u32;

INSERT INTO ts_coarse SELECT ts, toDateTime(ts, 'UTC'), toDateTime64(ts, 1, 'UTC'), toDateTime64(ts, 3, 'UTC'), value
FROM VALUES('ts UInt32, value Float64', (1000, 1), (1001, 2), (1002, 4), (1005, 3), (1006, 16), (1010, 32));

-- The grid: start 1000.5, end 1010.5, step 2.5 s, window 3.5 s, all with millisecond precision.
SELECT 'rate';
SELECT
    timeSeriesRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'increase';
SELECT
    timeSeriesIncreaseToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesIncreaseToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesIncreaseToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesIncreaseToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'irate';
SELECT
    timeSeriesInstantRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesInstantRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesInstantRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesInstantRateToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'deriv';
SELECT
    timeSeriesDerivToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesDerivToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesDerivToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesDerivToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'predict_linear';
SELECT
    timeSeriesPredictLinearToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3), 10)(ts_dt64_3, value) AS expected,
    timeSeriesPredictLinearToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3), 10)(ts_u32, value) = expected,
    timeSeriesPredictLinearToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3), 10)(ts_dt, value) = expected,
    timeSeriesPredictLinearToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3), 10)(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'changes';
SELECT
    timeSeriesChangesToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesChangesToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesChangesToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesChangesToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'resets';
SELECT
    timeSeriesResetsToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesResetsToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesResetsToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesResetsToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'last';
SELECT
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected,
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected,
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected,
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected
FROM ts_coarse;

SELECT 'sum, avg, count';
SELECT
    timeSeriesSumToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_sum,
    timeSeriesSumToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected_sum,
    timeSeriesAvgToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_avg,
    timeSeriesAvgToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected_avg,
    timeSeriesCountToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_count,
    timeSeriesCountToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) = expected_count
FROM ts_coarse;

SELECT 'max, min, ts_of_max, ts_of_min';
SELECT
    timeSeriesMaxToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_max,
    timeSeriesMaxToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) = expected_max,
    timeSeriesMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_min,
    timeSeriesMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) = expected_min,
    timeSeriesTimestampOfMaxToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_ts_of_max,
    timeSeriesTimestampOfMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value) AS expected_ts_of_min
FROM ts_coarse;

-- The timestamps of the maximums and minimums keep the type of the timestamps, so they are compared as seconds.
SELECT 'ts_of_max, ts_of_min with other timestamp types';
SELECT
    timeSeriesTimestampOfMaxToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_u32, value) AS ts_of_max_u32,
    CAST(ts_of_max_u32, 'Array(Nullable(Float64))') = CAST(timeSeriesTimestampOfMaxToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value), 'Array(Nullable(Float64))'),
    timeSeriesTimestampOfMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt, value) AS ts_of_min_dt,
    CAST(ts_of_min_dt, 'Array(Nullable(Float64))') = CAST(timeSeriesTimestampOfMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_3, value), 'Array(Nullable(Float64))'),
    timeSeriesTimestampOfMinToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(ts_dt64_1, value) AS ts_of_min_dt64_1
FROM ts_coarse;

-- Samples passed as arrays are converted too.
SELECT 'arrays';
SELECT
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(groupArray(ts_dt64_3), groupArray(value)) AS expected,
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(groupArray(ts_u32), groupArray(value)) = expected,
    timeSeriesLastToGrid(toDateTime64(1000.5, 3, 'UTC'), toDateTime64(1010.5, 3, 'UTC'), toDecimal64(2.5, 3), toDecimal64(3.5, 3))(groupArray((ts_dt, value))) = expected
FROM ts_coarse;

-- A grid with integer parameters still works with UInt32 timestamps.
SELECT 'integer parameters';
SELECT timeSeriesLastToGrid(1000, 1010, 5, 3)(ts_u32, value), timeSeriesRateToGrid(1000, 1010, 5, 3)(ts_dt, value) FROM ts_coarse;

DROP TABLE ts_coarse;
