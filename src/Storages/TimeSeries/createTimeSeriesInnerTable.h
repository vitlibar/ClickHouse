#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTViewTargets.h>

#include <base/UUID.h>


namespace DB
{
class ASTStorage;
class ColumnsDescription;
struct TimeSeriesSettings;

/// Creates an inner table and returns its StorageID.
StorageID createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings,
    ContextPtr context);

/// Returns a StorageID of an inner table.
StorageID getTimeSeriesInnerTableID(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    const StorageID & time_series_storage_id);

}
