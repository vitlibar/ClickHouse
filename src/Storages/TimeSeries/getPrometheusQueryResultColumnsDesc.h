#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
enum class PrometheusQueryResultType;
class ColumnsDescription;
struct StorageID;

/// Returns the description of columns returned by the prometheusQuery() table function for a specified promql query.
ColumnsDescription getPrometheusQueryResultColumnsDesc(PrometheusQueryResultType promql_query_result_type,
                                                       const ContextPtr & context,
                                                       const StorageID & time_series_storage_id);

}
