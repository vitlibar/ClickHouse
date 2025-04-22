#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
class ParsedPrometheusQuery;
class ColumnsDescription;
struct StorageID;

/// Returns the description of columns returned by the prometheusQuery() table function for a specified promql query.
ColumnsDescription getPrometheusQueryResultColumnsDesc(
    const ParsedPrometheusQuery & promql_query, const StorageID & time_series_storage_id, const ContextPtr & context);

}
