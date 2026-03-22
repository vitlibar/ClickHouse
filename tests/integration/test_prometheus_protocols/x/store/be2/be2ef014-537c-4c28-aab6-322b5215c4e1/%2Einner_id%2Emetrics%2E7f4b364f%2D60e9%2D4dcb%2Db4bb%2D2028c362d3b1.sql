ATTACH TABLE _ UUID '0c5185db-c2e2-465f-ba6d-ac3213d7357c'
(
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
SETTINGS index_granularity = 8192
