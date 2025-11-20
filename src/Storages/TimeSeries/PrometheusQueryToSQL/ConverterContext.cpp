#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultSortingByDefault.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(const PQT & promql_tree_, const PrometheusQueryEvaluationSettings & settings_)
    : promql_tree(promql_tree_)
    , time_series_storage_id(settings_.time_series_storage_id)
    , timestamp_scale(getTimeseriesScale(settings_.result_timestamp_type))
    , result_timestamp_type(settings_.result_timestamp_type)
    , result_scalar_type(settings_.result_scalar_type)
    , limit(settings_.limit)
    , node_evaluation_range_getter(promql_tree_, settings_)
    , result_sorting(getResultSortingByDefault(promql_tree_, settings_))
{
}

}
