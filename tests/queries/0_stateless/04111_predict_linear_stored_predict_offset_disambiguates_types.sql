-- Regression test: two `timeSeriesPredictLinearToGrid` states that differ only by `predict_offset`
-- must have different `AggregateFunction(...)` type names. Before the fix, `predict_offset` was not
-- stored in the state type, so states with different offsets silently merged and produced wrong results.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_pred_data;

CREATE TABLE ts_pred_data (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ts_pred_data VALUES
    ('1970-01-01 00:01:41', 10101),
    ('1970-01-01 00:01:47', 10107),
    ('1970-01-01 00:02:00', 10120);

-- Two states with different predict_offset (5 vs 10). The type names must differ.
SELECT toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value))
     = toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, 10)(timestamp, value)) AS types_equal
FROM ts_pred_data LIMIT 1;

-- Equivalent spellings of the same predict_offset (5, 5.0, toFloat64(5)) must produce the same type.
SELECT toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value))
     = toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5.0)(timestamp, value)) AS types_equal
FROM ts_pred_data LIMIT 1;

SELECT toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value))
     = toTypeName(timeSeriesPredictLinearToGridState(100, 200, 10, 15, toFloat64(5))(timestamp, value)) AS types_equal
FROM ts_pred_data LIMIT 1;

DROP TABLE ts_pred_data;
