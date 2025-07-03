#pragma once


namespace DB
{

/// Converts a parsed prometheus query to SQL.
ASTPtr prometheusQueryToSQL(const PrometheusQueryTree & promql,
                            const StorageID & time_series_table_id,
                            const PrometheusQueryTree::Timestamp & evaluation_time,
                            const PrometheusQueryTree::OffsetType & lookback_delta);

ColumnsWithTypesAndNames getPrometheusQueryResultColumns(const PrometheusQueryTree & promql,
                                                         const StorageID & time_series_table_id,
                                                         const ContextPtr & context);
    
}