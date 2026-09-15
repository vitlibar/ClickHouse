#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <DataTypes/DataTypesDecimal.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , time_series_version(settings_.time_series_version)
    , table_timestamp_type(settings_.table_timestamp_type)
    , table_timestamp_scale(tryGetDecimalScale(*table_timestamp_type).value_or(0))
    , result_timestamp_type(settings_.timestamp_type)
    , result_timestamp_scale(getDecimalScale(*result_timestamp_type))
    , result_type(getResultType(*promql_tree_, settings_))
    , node_range_getter(promql_tree_, settings_)
{
    if (promql_tree->getTimestampScale() != result_timestamp_scale)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "PromQL query was parsed with timestamp scale {} but the evaluation settings use timestamp scale {}",
                        promql_tree->getTimestampScale(), result_timestamp_scale);
    }
}

}
