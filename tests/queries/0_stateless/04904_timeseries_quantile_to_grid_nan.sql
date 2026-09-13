-- Tests how the timeSeriesQuantileToGrid aggregate function treats NaN samples. They follow Prometheus, whose
-- `vectorByValueHeap.Less` sorts NaN before every real value instead of dropping it, so a window of [1, NaN, 2]
-- sorts as [NaN, 1, 2] and its median is 1 rather than the 1.5 of a NaN-free [1, 2].

SET enable_time_series_aggregate_functions = 1;

SELECT '-- a NaN sample is kept and sorts before every real value';
-- Grid [100, 110] with a 30 second window: the first grid point sees only (100, 1),
-- the second one sees all three samples.
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 0.5);
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, [0.5, 0.5]);

SELECT '-- phi = 0 selects the NaN that sorts first, phi = 1 selects the largest real value';
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 0);
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, 1);
-- The same with one level per grid point: phi = 0.5 at the first grid point, phi = 0 at the second one.
WITH [100, 105, 110]::Array(DateTime) AS timestamps, [1, nan, 2]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 110, 10, 30)(timestamps, values, [0.5, 0]);

SELECT '-- a window with only NaN samples yields NaN, a window without samples stays NULL';
-- Grid [100, 120, 140] with a 30 second window: the last grid point has no samples at all.
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 140, 20, 30)(timestamps, values, 0.5);
WITH [100, 110]::Array(DateTime) AS timestamps, [nan, nan]::Array(Float64) AS values
SELECT timeSeriesQuantileToGrid(100, 140, 20, 30)(timestamps, values, [0.5, 0.5, 0.5]);
