-- Tags: no-fasttest, no-replicated-database
-- ^^ ANTLR4 support is disabled in the fast-test build, and the PromQL
-- grammar requires it. The experimental TimeSeries table engine does not
-- round-trip through DatabaseReplicated.

-- A fixed `@` modifier on the range vector freezes the sample window at the `@` timestamp. For most range
-- functions the whole call is then step-invariant and PromQL evaluates it once, repeating the result over
-- the range-query grid (as the shared `rate`/`increase`/... path does); `quantile_over_time` follows that rule.
-- `predict_linear` does not: its result depends on the evaluation time, so Prometheus lists it among the
-- functions unsafe under `@` and evaluates it at every step against the frozen window. The prediction at the
-- step `t` is the fit at the frozen timestamp, extrapolated to `t` and then by the horizon.
-- Both functions used to slide the aggregate window over the outer evaluation timestamps instead, so the
-- result changed from step to step and eventually became NULL.

SET enable_time_series_table = 1;
SET enable_time_series_aggregate_functions = 1;

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- Linear ramp 10 -> 20 -> 30 (slope +1/6 per second), ending at the `@` timestamp 1700000000.
INSERT INTO ts (metric_name, tags, time_series) VALUES
    ('up', map('instance', 'host1'), [(toDateTime64(1699999880, 3), 10.0), (toDateTime64(1699999940, 3), 20.0), (toDateTime64(1700000000, 3), 30.0)]);

-- Every grid point of the range below is past the last sample, so without the fixed-@ handling the
-- window slides off the samples and the result decays to a single non-NULL step (or none at all).

-- The window is frozen at 1700000000, and the prediction at the step `t` is 30 + (t - 1700000000 + 60) / 6.
SELECT 'predict_linear with a fixed @, range query: the frozen fit extrapolated to every step:';
SELECT tags, arrayMap(x -> round(x.2, 3), time_series) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, 60)', 1700000100, 1700000400, 100)
ORDER BY ALL;

-- Evaluated exactly at the `@` timestamp, so window time and evaluation time coincide: 30 + 60/6 = 40.
SELECT 'predict_linear with a fixed @ at the evaluation time: no shift, 40:';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up[3m] @ 1700000000, 60)', 1700000000) ORDER BY ALL;

-- Same frozen window, evaluated 100s later: 30 + (100 + 60) / 6.
SELECT 'predict_linear with a fixed @, instant query 100s later:';
SELECT tags, value FROM prometheusQuery(ts, 'predict_linear(up[3m] @ 1700000000, 60)', 1700000100) ORDER BY ALL;

-- A quantile of a frozen window has no evaluation-time term at all, so it is the same at every step.
SELECT 'quantile_over_time with a fixed @, range query: the median of 10/20/30, repeated:';
SELECT tags, arrayMap(x -> x.2, time_series) AS values
FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, up[3m] @ 1700000000)', 1700000100, 1700000400, 100)
ORDER BY ALL;

SELECT 'quantile_over_time with a fixed @: exactly one distinct value across all steps:';
SELECT length(arrayDistinct(arrayMap(x -> x.2, time_series)))
FROM prometheusQueryRange(ts, 'quantile_over_time(0.5, up[3m] @ 1700000000)', 1700000100, 1700000400, 100)
ORDER BY ALL;

-- The horizon may vary with the step as well. `60 + 0 * time()` is 60 at every step (it varies with `time()`
-- only formally) and must give the same result as the constant 60 above; `time()` is the step itself.
SELECT 'predict_linear with a fixed @ and a varying horizon: per-step predictions from the frozen fit:';
SELECT tags, arrayMap(x -> round(x.2, 3), time_series) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, 60 + 0 * time())', 1700000100, 1700000400, 100)
ORDER BY ALL;
SELECT tags, arrayMap(x -> round(x.2, 3), time_series) AS values
FROM prometheusQueryRange(ts, 'predict_linear(up[3m] @ 1700000000, time())', 1700000100, 1700000400, 100)
ORDER BY ALL;

-- `timeSeriesQuantileToGrid` derives its window from each grid point and cannot express a frozen
-- window with a per-point quantile level, so this combination is rejected instead of returning sliding-window results.
SELECT * FROM prometheusQueryRange(ts, 'quantile_over_time(time(), up[3m] @ 1700000000)', 1700000100, 1700000400, 100); -- { serverError NOT_IMPLEMENTED }

DROP TABLE ts;
