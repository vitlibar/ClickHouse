ATTACH TABLE _ UUID 'f7d6bdc9-b41d-4c65-91ea-c270de7f0389'
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
SETTINGS index_granularity = 8192
