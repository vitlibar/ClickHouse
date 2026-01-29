#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(std::shared_ptr<const PrometheusQueryTree> promql_tree_,
                                   const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , timestamp_data_type(settings_.timestamp_data_type)
    , timestamp_scale(settings_.timestamp_scale)
    , value_data_type(settings_.value_data_type)
    , node_evaluation_range_getter(promql_tree_, settings_)
    , result_type(getResultType(*promql_tree_, settings_))
{
}

}
