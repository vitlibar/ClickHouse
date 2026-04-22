-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101407
-- `SELECT ... FINAL` on an AggregatingMergeTree with a `timeSeriesPredictLinearToGrid` state column
-- must not throw: a part merge reconstructs the function from its state type, which requires
-- the 5th parameter `predict_offset` to be present in the type metadata.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_pred_data;
DROP TABLE IF EXISTS ts_pred_agg;

CREATE TABLE ts_pred_data (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ts_pred_data VALUES
    ('1970-01-01 00:01:41', 10101),
    ('1970-01-01 00:01:47', 10107),
    ('1970-01-01 00:02:00', 10120);

CREATE TABLE ts_pred_agg ENGINE = AggregatingMergeTree() ORDER BY k AS
    SELECT 1 AS k, timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value) AS agg
    FROM ts_pred_data;

-- A second insert produces a second part so that FINAL has something to merge.
INSERT INTO ts_pred_agg
    SELECT 1 AS k, timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value) AS agg
    FROM ts_pred_data;

-- FINAL forces reconstruction of the aggregate function from the stored state type during merge.
SELECT k, arrayMap(x -> round(x, 4), timeSeriesPredictLinearToGridMerge(100, 200, 10, 15, 5)(agg))
    FROM ts_pred_agg FINAL GROUP BY k ORDER BY k;

DROP TABLE ts_pred_agg;
DROP TABLE ts_pred_data;
