---
slug: /en/engines/table-engines/special/time_series
sidebar_position: 60
sidebar_label: TimeSeries
---

# TimeSeries Engine (experimental)

A table engine storing time series, i.e. a set of values associated with timestamps and tags (or labels):

```
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

## Syntax {#syntax}

``` sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[DATA db.data_table_name | DATA ENGINE data_table_engine(arguments)]
[TAGS db.tags_table_name | DATA ENGINE tags_table_engine(arguments)]
[METRICS db.metrics_table_name | DATA ENGINE metrics_table_engine(arguments)]
```

## Usage {#usage}

It's easier to start with everything set by default (it's allowed to create a `TimeSeries` table without specifying a list of columns):

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Then this table can be used with the following protocols (a port must be assigned in the server configuration):
- [prometheus remote-write](../../../interfaces/prometheus.md#remote-write)
- [prometheus remote-read](../../../interfaces/prometheus.md#remote-read)

## Target tables {#target-tables}

A `TimeSeries` table doesn't have its own data, everything is stored in its target tables.
This is similar to how a [materialized view](../../../sql-reference/statements/create/view#materialized-view) work,
with the difference that a materialized view has one target table
whereas a `TimeSeries` table has three target tables named _data_, _tags_, and _metrics_.

The target tables can be either specified explicitly in the `CREATE TABLE` query
or the `TimeSeries` table engine can generate inner target tables automatically.

## Actual structure {#actual-structure}

There are multiple ways to create a table with the `TimeSeries` table engine.
The simplest statement

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
```

will actually create the following table (you can see that by executing `SHOW CREATE TABLE my_table`):

``` sql
CREATE TABLE my_table
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
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = TimeSeries
DATA ENGINE = MergeTree ORDER BY (id, timestamp)
DATA INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
TAGS ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id)
TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
METRICS ENGINE = ReplacingMergeTree ORDER BY metric_family_name
METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

So the columns were generated automatically and also there are three inner UUIDs in this statement -
one per each inner target table that was created.
(Inner UUIDs are not shown normally until setting
[show_table_uuid_in_table_create_query_if_not_nil](../../../operations/settings/settings#show_table_uuid_in_table_create_query_if_not_nil)
is set.)

## Target tables {#target-tables}

Inner target tables have names like `.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
and each target table has columns which is a subset of the columns of the main `TimeSeries` table:

``` sql
CREATE TABLE default.`.inner_id.data.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

``` sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
```

``` sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

If rows are inserted to a TimeSeries table they will be actually stored in those three target tables.
The _data_ table contains time series associated with some identifier.
The _tags_ table contains those identifiers calculated for each combination of a metric name and tags.
And the _metrics_ table contains some information about metrics been collected, the types of those metrics and their descriptions.

## The `id` column {#inner-target-tables}

The `id` column contains identifiers, every identifier is calculated for a combination of a metric name and tags.
The DEFAULT expression for the `id` column is a formulae used for that calculation.
Both the type of the `id` column and that formulae can be adjusted by specifying them explicitly:

``` sql
CREATE TABLE my_table (id UInt64 DEFAULT sipHash64(metric_name, all_tags) ENGINE=TimeSeries
```

## The `tags` and `all_tags` columns {#inner-target-tables}

There are two columns containing tags - `tags` and `all_tags`. In this example they mean the same, however they can be different
if setting `tags_to_columns` is used. This setting allows to specify that a specific tag should be stored in a separate column instead of storing
in a map inside the `tags` column:

``` sql
CREATE TABLE my_table ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

This statement will add columns
```
    `instance` String,
    `job` String
```
to the definition of both `my_table` and its inner _tags_ target table. In this case the `tags` column will not contain tags `instance` and `job`,
but the `all_tags` column will contain them. The `all_tags` column is ephemeral and its only purpose to be used in the DEFAULT expression
for the `id` column.

The types of columns can be adjusted by specifying them explicitly:

``` sql
CREATE TABLE my_table (instance LowCardinality(String), job LowCardinality(Nullable(String)))
ENGINE=TimeSeries SETTINGS = {'instance': 'instance', 'job': 'job'}
```

## Table engines of inner target tables {#table-engines-of-inner-target-tables}

By default inner target tables use the following table engines:
- the _data_ table uses [MergeTree](../mergetree-family/mergetree);
- the _tags_ table uses [AggregatingMergeTree](../mergetree-family/aggregatingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates, and also because it's required to do aggregation for columns `min_time` and `max_time`;
- the _metrics_ table uses [ReplacingMergeTree](../mergetree-family/replacingmergetree) because the same data is often inserted multiple times to this table so we need a way
to remove duplicates.

Other table engines also can be used for inner target tables if it's specified so:

``` sql
CREATE TABLE my_table ENGINE=TimeSeries
DATA ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

## Not-inner target tables {#not-inner-target-tables}

It's possible to make a `TimeSeries` table use a manually created table:

``` sql
CREATE TABLE data_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries DATA=data_for_my_table TAGS=tags_for_my_table METRICS=metrics_for_my_table;
```

## Settings {#settings}

Here is a list of settings which can be specified while defining a `TimeSeries` table:

| Name | Type | Default | Description |
|---|---|---|---|
| `tags_to_columns` | Map | {} | Map specifying which tags should be put to separate columns in the _tags_ table. Syntax: `{'tag1': 'column1', 'tag2' : column2, ...}` |
| `store_other_tags_as_map` | Bool | true | If set to true then all tags unspecified to the `tags_to_column` setting will be stored in a column named `tags`. If set to false then only tags specified in the `tags_to_column` setting will be allowed |
| `enable_column_all_tags` | Bool | true | If set to true then the table will contain a column named `all_tags`, containing all tags, including both those ones which are specified in the `tags_to_columns` setting and those ones which are stored in the `tags` column. The `all_tags` column is not stored anywhere, it's generated on the fly |
| `copy_id_default_to_tags_table` | Bool | true | When creating an inner target `tags` table, this flag enables setting the default expression for the `id` column |
| `create_ephemeral_all_tags_in_tags_table` | Bool | true | When creating an inner target _tags_ table, this flag enables creating an ephemeral column named `all_tags` |
| `store_min_time_and_max_time` | Bool | true | If set to true then the table will store `min_time` and `max_time` for each time series |
| `filter_by_min_time_and_max_time` | Bool | true | If set to true then the table will use the `min_time` and `max_time` columns for filtering time series |
| `aggregate_min_time_and_max_time` | Bool | true | When creating an inner target `tags` table, this flag enables using `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` instead of just `Nullable(DateTime64(3))` as the type of the `min_time` column, and the same for the `max_time` column |

# Functions {#functions}

Here is a list of functions supporting a `TimeSeries` table as an argument:
- [timeSeriesData](../../../sql-reference/table-functions/timeSeriesData.md)
- [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
- [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)
