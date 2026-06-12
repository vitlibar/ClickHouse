-- Drives the sliding two-stack aggregation path (`fillGridResultsByTwoStacks`), used when a window spans at
-- least each function's `TWO_STACKS_BUCKETS_PER_WINDOW_THRESHOLD` buckets (at most 44). Both scenarios below
-- span 50 and 51 buckets per window — above every function's threshold — so they drive the two-stack path for
-- every timeSeries*ToGrid function, and the window slides across the data so buckets both enter and leave.
-- Covers a whole-multiple window (`window % step == 0`) and a window that splits each step (`window % step != 0`).
SET allow_experimental_time_series_aggregate_functions = 1;
SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_two_stacks;
CREATE TABLE ts_two_stacks (timestamp DateTime, value Float64) ENGINE = MergeTree ORDER BY timestamp;
-- Dense-ish series spanning the windows' reach back to 60 (T_0 - window); includes resets at 88 and 108.
INSERT INTO ts_two_stacks VALUES
    (60, 1), (65, 3), (72, 6), (80, 10), (88, 9), (95, 14), (101, 20), (108, 5), (114, 8), (120, 13);

-- step=1 over [100,120] -> 21 grid points. window=50 -> 50 buckets/window (>= threshold -> two-stacks); window % step == 0.
SELECT 'two-stacks, window multiple of step (window=50, step=1):';
SELECT timeSeriesResampleToGridWithStaleness(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesChangesToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesResetsToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesRateToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDeltaToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantRateToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantDeltaToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDerivToGrid(100, 120, 1, 50)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesPredictLinearToGrid(100, 120, 1, 50, 10)(timestamp, value) FROM ts_two_stacks;

-- step=2 over [100,120] -> 11 grid points. window=51 -> 51 buckets/window (>= threshold -> two-stacks); window % step == 1, so each step is split.
SELECT 'two-stacks, window splits step (window=51, step=2):';
SELECT timeSeriesResampleToGridWithStaleness(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesChangesToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesResetsToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesRateToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDeltaToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantRateToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesInstantDeltaToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesDerivToGrid(100, 120, 2, 51)(timestamp, value) FROM ts_two_stacks;
SELECT timeSeriesPredictLinearToGrid(100, 120, 2, 51, 10)(timestamp, value) FROM ts_two_stacks;

DROP TABLE ts_two_stacks;
