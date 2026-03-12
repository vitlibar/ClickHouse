#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>


namespace DB
{
class ColumnsDescription;
struct TimeSeriesSettings;

/// Checks that a target table used by a TimeSeries table has all the required columns.
/// Throws an exception if a required column doesn't exist.
void checkTimeSeriesTargetTable(
    const StorageID & target_table_id,
    const ColumnsDescription & target_table_columns,
    ViewTarget::Kind target_kind,
    const TimeSeriesSettings & time_series_settings);

}
