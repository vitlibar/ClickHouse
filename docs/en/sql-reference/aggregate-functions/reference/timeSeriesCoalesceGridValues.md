---
description: 'Combines non-NULL values in arrays from different rows into a single array.'
sidebar_position: 146
slug: /sql-reference/aggregate-functions/reference/timeSeriesCoalesceGridValues
title: 'timeSeriesCoalesceGridValues'
doc_type: 'reference'
---

# timeSeriesCoalesceGridValues

The function finds non-NULL values in the source array of `values`, and then combines
such non-NULL values extracted from different rows into a single result array
retaining the positions of these values. For example,

```sql
CREATE TABLE mytable(values Array(Nullable(Float64))) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO mytable VALUES ([100, NULL, NULL, NULL]);
INSERT INTO mytable VALUES ([NULL, 200, NULL, NULL]);
INSERT INTO mytable VALUES ([NULL, NULL, 300, NULL]);
SELECT timeSeriesCoalesceGridValues('any')(values) AS result FROM mytable;
```

```response
┌─result────────────────┐
│ [100, 200, 300, NULL] │
└───────────────────────┘
```

If all the arrays extracted from the rows have NULL at a specific position then the function returns an array with NULL at that position as well. The parameter `mode` sets what the function should do if two or more rows have non-null values at the same position.

The main purpose of this function is to be applied to the result of `timeSeries*toGrid` functions,
(for example [timeSeriesRateToGrid()](./timeSeriesRateToGrid.md), [timeSeriesLastToGrid()](./timeSeriesResampleToGridWithStaleness.md)) after modifying the `group` column
with functions like [timeSeriesRemoveTag()](../../functions/time-series-functions#timeSeriesRemoveTag) or
[timeSeriesRemoveAllTagsExcept()](../../functions/time-series-functions#timeSeriesRemoveAllTagsExcept).

**Syntax**

```sql
timeSeriesCoalesceGridValues(mode)(values [, group])
```

**Parameters**
- `mode` - sets what the function should do in case two or more of the arrays extracted from different rows have values which are not `NULL` at the same position. The following values of the `mode` are supported:

| `mode` | Description |
| --- | --- |
| `'any'` | The function returns the first non-null value it meets at each position. |
| `'nan'` | The function returns `NaN` if there are multiple non-null values at the same position (even if these values are equal) |
| `'throw'` | The function throws an exception (`Found duplicate series`) if there are multiple non-null values at the same position (even if these values are equal), with optional information about these time series in case the argument `group` is provided |

**Arguments**

- `values` - array of nullable floating-point values
- `group` - [optional] the groups of the corresponding time series (see the function [timeSeriesTagsToGroup()](../../functions/time-series-functions#timeSeriesTagsToGroup))

**Returned value**

The function returns a new array of nullable floating-point values. 

**Example**

```sql
CREATE TABLE test(values Array(Nullable(Float64))) ENGINE=Memory;
INSERT INTO test VALUES ([1, NULL, 15, NULL, NULL, 4]);
INSERT INTO test VALUES ([NULL, 7, NULL, 8, NULL, 4]);
SELECT timeSeriesCoalesceGridValues('any')(values) AS result FROM test;
```

Response:

```response
┌─timeSeriesCoalesceGridValues('any')(values)─┐
│ [1, 7, 15, 8, NULL, 4]                      │
└─────────────────────────────────────────────┘
```

The last element rows presents in both rows, so if we set `mode` to `nan` the function will return `NaN` at the last position:


```sql
SELECT timeSeriesCoalesceGridValues('nan')(values) AS result FROM test;
```

Response:

```response
┌─timeSeriesCoalesceGridValues('nan')(values)─┐
│ [1, 7, 15, 8, NULL, nan ]                   │
└─────────────────────────────────────────────┘
```

And setting `mode` to `throw` will make the function throw an exception:

```sql
SELECT timeSeriesCoalesceGridValues('throw')(values) AS result FROM test;
```

Response:

```response
DB::Exception: Instant vector cannot contain metrics with the same groups of tags: found duplicate series
```

Optional argument `group` can be used to provide extra information in the error message:

```sql
CREATE TABLE test(values Array(Nullable(Float64)), tags Array(Tuple(String, String))) ENGINE=Memory;
INSERT INTO test VALUES ([1, NULL, 15, NULL, NULL, 4], [('__name__', 'up')]);
INSERT INTO test VALUES ([NULL, 7, NULL, 8, NULL, 4], [('__name__', 'up')]);
SELECT timeSeriesCoalesceGridValues('throw')(values, timeSeriesTagsToGroup(tags)) FROM test;
```

Response:

```response
DB::Exception: Instant vector cannot contain metrics with the same groups of tags: found duplicate series with {'__name__': 'up'}
```

:::note
This function is experimental, enable it by setting `allow_experimental_ts_to_grid_aggregate_function=true`.
:::
