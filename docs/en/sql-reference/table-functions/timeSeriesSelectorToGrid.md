---
description: 'Reads time series from a TimeSeries table filtered by a selector and with timestamps in a specified interval.'
sidebar_label: 'timeSeriesSelectorToGrid'
sidebar_position: 145
slug: /sql-reference/table-functions/timeSeriesSelectorToGrid
title: 'timeSeriesSelectorToGrid'
doc_type: 'reference'
---

# timeSeriesSelectorToGrid Table Function

Reads time series from a TimeSeries table filtered by a selector and with timestamps in all these windows:
```
/// TIME ---------->
///
/// (start_time - window)      start_time      (start_time + step - window)      (start_time + step)      (start_time + 2 * step - window)      (start_time + 2 * step)      ...      (end_time - window)      end_time
///  ************************************       ****************************************************       ************************************************************                ********************************
///             first window                                     second window                                                 third window                                                      last window
```

All these windows are always left-opened and right-closed, i.e. for each returned row one of the following inequations is always true:
```
start_time - window < timestamp <= start_time
start_time + step - window < timestamp <= start_time + step
start_time + 2 * step - window < timestamp <= start_time + 2 * step
...
```

This function is similar to function [timeSeriesSelector()](./timeSeriesSelector.md), and the following call
```sql
timeSeriesSelector('table_name', 'instant_selector', min_time, max_time)
```
returns the same result as
```sql
timeSeriesSelectorToGrid('table_name', 'instant_selector', max_time, max_time, 0, max_time - min_time + eps)
```
where `eps` is the smallest positive duration (usually it equals to one millisecond).

## Syntax {#syntax}

```sql
timeSeriesSelectorToGrid('db_name', 'time_series_table', 'instant_selector', start_time, end_time, step, window)
timeSeriesSelectorToGrid(db_name.time_series_table, 'instant_selector', start_time, end_time, step, window)
timeSeriesSelectorToGrid('time_series_table', 'instant_selector', start_time, end_time, step, window)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `instant_selector` - An instant selector written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), without `@` or `offset` modifiers.
- `start time` - Specifies start of the grid.
- `end time` - Specifies end of the grid.
- `step` - Specifies step of the grid in seconds.
- `window` - Specifies the maximum "staleness" in seconds of the considered samples. The staleness window is always left-opened and right-closed.

## Returned value {#returned_value}

The function returns three columns:
- `id` - Contains the identifiers of time series matching the specified selector.
- `timestamp` - Contains timestamps.
- `value` - Contains values.

The function returns data in unspecified order.

## Example {#example}

```sql
SELECT * FROM timeSeriesSelectorToGrid(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE, INTERVAL 30 SECONDS)
```
