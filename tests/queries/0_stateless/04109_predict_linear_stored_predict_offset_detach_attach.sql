-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/102481
-- A table with a `timeSeriesPredictLinearToGrid` aggregate column must survive DETACH/ATTACH:
-- the 5th parameter `predict_offset` has to be stored in the state type.

SET allow_experimental_ts_to_grid_aggregate_function = 1;

DROP TABLE IF EXISTS ts_pred_data;
DROP TABLE IF EXISTS ts_pred_agg;

CREATE TABLE ts_pred_data (timestamp DateTime('UTC'), value Float64) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO ts_pred_data VALUES
    ('1970-01-01 00:01:41', 10101),
    ('1970-01-01 00:01:47', 10107),
    ('1970-01-01 00:02:00', 10120);

-- CTAS infers the column type from the aggregate function's state type.
CREATE TABLE ts_pred_agg ENGINE = AggregatingMergeTree() ORDER BY k AS
    SELECT 1 AS k, timeSeriesPredictLinearToGridState(100, 200, 10, 15, 5)(timestamp, value) AS agg
    FROM ts_pred_data;

-- The stored state type must carry all 5 parameters; before the fix only 4 were stored.
SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = 'ts_pred_agg' AND name = 'agg' AND type LIKE '%timeSeriesPredictLinearToGrid(%,%,%,%,%)%';

-- DETACH/ATTACH round-trip exercises reconstruction from the stored metadata.
DETACH TABLE ts_pred_agg;
ATTACH TABLE ts_pred_agg;

-- After ATTACH, querying through the -Merge combinator still works.
SELECT k, arrayMap(x -> round(x, 4), timeSeriesPredictLinearToGridMerge(100, 200, 10, 15, 5)(agg)) FROM ts_pred_agg GROUP BY k ORDER BY k;

DROP TABLE ts_pred_agg;
DROP TABLE ts_pred_data;
