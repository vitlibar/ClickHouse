#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRangeGetter.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLSubquery.h>


namespace DB
{
    class PrometheusQueryTree;
}


namespace DB::PrometheusQueryToSQL
{

struct ConverterConfig;

/// Contains information used for converting prometheus query to SQL query.
struct ConverterContext
{
    const StorageID time_series_storage_id;
    const PrometheusQueryTree & promql_tree;
    const DataTypePtr timestamp_type;
    const UInt32 timestamp_scale;
    const DataTypePtr scalar_type;
    const DecimalField<Decimal64> lookback_delta;
    const DecimalField<Decimal64> default_resolution;
    const std::optional<size_t> limit;
    SQLSubqueries subqueries;
    NodeEvaluationRangeGetter node_evaluation_range_getter;

    ConverterContext(const PrometheusQueryTree & promql_tree_, const ConverterConfig & config_);
};

}
