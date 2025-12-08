#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>


namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if a prometheus query allows evaluating over a range and throws an exception if not.
    void checkPrometheusQueryAllowsEvaluationRange(const PQT & promql_tree)
    {
        if ((promql_tree.getResultType() == ResultType::SCALAR)
            || (promql_tree.getResultType() == ResultType::INSTANT_VECTOR))
            return;

        throw Exception(ErrorCodes::BAD_ARGUMENTS,
                        "Invalid expression type {} for range query, must be {} or {}",
                        promql_tree.getResultType(),
                        ResultType::SCALAR,
                        ResultType::INSTANT_VECTOR);
    }
}


ResultType getResultType(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    if (settings.evaluation_time)
    {
        return promql_tree.getResultType();
    }
    else if (settings.evaluation_range)
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        return ResultType::RANGE_VECTOR;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");
    }
}


DataTypePtr getResultTimestampType(const PrometheusQueryEvaluationSettings & settings)
{
    auto timestamp_type = settings.data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    UInt32 timestamp_scale = std::min<UInt32>(getTimeseriesScale(timestamp_type), 3);
    String timezone = getTimeseriesTimezone(timestamp_type);
    return getTimeseriesTimeType(timestamp_scale, timezone);
}

UInt32 getResultTimestampScale(const PrometheusQueryEvaluationSettings & settings)
{
    return getResultTimestampScale(settings.data_table_metadata);
}

UInt32 getResultTimestampScale(const StorageMetadataPtr & data_table_metadata)
{
    auto timestamp_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    return std::min<UInt32>(getTimeseriesScale(timestamp_type), 3);
}

DataTypePtr getResultScalarType(const PrometheusQueryEvaluationSettings &)
{
    return std::make_shared<DataTypeFloat64>();
}

}
