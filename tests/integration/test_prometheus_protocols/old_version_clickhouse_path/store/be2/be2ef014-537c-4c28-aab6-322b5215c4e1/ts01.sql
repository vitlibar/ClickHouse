ATTACH TABLE _ UUID '7f4b364f-60e9-4dcb-b4bb-2028c362d3b1'
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String),
    `min_time` Nullable(DateTime64(3)),
    `max_time` Nullable(DateTime64(3)),
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries DATA INNER UUID 'f7d6bdc9-b41d-4c65-91ea-c270de7f0389' DATA
ENGINE = MergeTree
ORDER BY (id, timestamp) TAGS INNER UUID '7531f08a-04b8-4184-ab8c-5a1df1c12dba' TAGS
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY tuple(metric_name, id) METRICS INNER UUID '0c5185db-c2e2-465f-ba6d-ac3213d7357c' METRICS
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
