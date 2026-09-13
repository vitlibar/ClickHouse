-- Tests the `phi` argument of timeSeriesQuantileToGrid: one quantile level for the whole grid or one level per grid
-- point, the value types it accepts, and the requirement that it is the same in every row and in every merged state.
-- The PromQL function built on it is tested end to end in 04551_promql_phase3_range_functions.

DROP TABLE IF EXISTS quantile_input;

-- The function is in private preview and disabled by default.
SET enable_time_series_aggregate_functions = 0;
SET enable_time_series_table = 0;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(toDateTime(100), 10::Float64, 0.5); -- { serverError UNKNOWN_AGGREGATE_FUNCTION }

SET enable_time_series_aggregate_functions = 1;

-- Samples (100, 10), (110, 20), (120, 30) on the grid [100, 110, 120] with a staleness window of 30 seconds, so the
-- windows hold {10}, {10, 20} and {10, 20, 30}. `grp` splits the samples into two partial states for the merge tests below.
CREATE TABLE quantile_input (grp UInt8, timestamp DateTime, value Float64, phis Array(Float64)) ENGINE = MergeTree ORDER BY timestamp;
INSERT INTO quantile_input VALUES (0, 100, 10, [0, 0.5, 1]), (0, 110, 20, [0, 0.5, 1]), (1, 120, 30, [0, 0.5, 1]);

SELECT '-- one quantile level for the whole grid';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 0.5) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 1) FROM quantile_input;
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(arrayZip(timestamps, values), 0.5);

SELECT '-- one quantile level per grid point';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, phis) FROM quantile_input;
WITH [100, 110, 120]::Array(DateTime) AS timestamps, [10, 20, 30]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamps, values, [0., 0.5, 1.]);
-- An array with the same level at every grid point is the same as that level.
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0.5, 0.5, 0.5]) FROM quantile_input;

SELECT '-- the level accepts any numbers: Float32, Float64 and integers';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value::Float32, phis::Array(Float32)) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 1::UInt8) FROM quantile_input;
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0, 1, 1]::Array(UInt64)) FROM quantile_input;

SELECT '-- partial states carrying the same level merge';
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, phis) AS st FROM quantile_input GROUP BY grp);
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, 0.5) AS st FROM quantile_input GROUP BY grp);

SELECT '-- the level must be the same in every row and in every merged state';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, 0.5 + grp) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
-- Rows that differ only in the middle of the array must be rejected as well.
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, arrayMap(x -> if(x = 0.5, x + grp * 0.2, x), phis)) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, 0.5 + grp) AS st FROM quantile_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGridMerge(100, 120, 10, 30)(st) FROM (SELECT timeSeriesQuantileToGridState(100, 120, 10, 30)(timestamp, value, arrayMap(x -> x / (1 + grp), phis)) AS st FROM quantile_input GROUP BY grp); -- { serverError BAD_ARGUMENTS }

SELECT '-- invalid arguments';
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, [0., 0.5]) FROM quantile_input; -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, phis::Array(Nullable(Float64))) FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value, '0.5') FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30)(timestamp, value) FROM quantile_input; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT timeSeriesQuantileToGrid(100, 120, 10, 30, 0.5)(timestamp, value) FROM quantile_input; -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE quantile_input;
