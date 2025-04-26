#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
class PrometheusQueryTree;
class ColumnsDescription;
struct StorageID;

/// Returns the description of columns returned by the prometheusQuery() table function for a specified promql query.
ColumnsDescription getPrometheusQueryResultColumnsDesc(
    const PrometheusQueryTree & promql_query, const StorageID & time_series_storage_id, const ContextPtr & context);

}
