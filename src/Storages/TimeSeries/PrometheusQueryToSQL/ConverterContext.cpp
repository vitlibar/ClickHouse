#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterConfig.h>


namespace DB::PrometheusQueryToSQL
{

ConverterContext::ConverterContext(const PrometheusQueryTree & promql_tree_, const ConverterConfig & config_)
    : time_series_storage_id(config_.time_series_storage_id)
    , promql_tree(promql_tree_)
    , timestamp_type(config_.timestamp_type)
    , timestamp_scale(getTimeSeriesTimestampScale(timestamp_type))
    , scalar_type(config_.scalar_type)
    , lookback_delta(getTimeSeriesInterval(config_.lookback_delta, timestamp_scale))
    , default_resolution(getTimeSeriesInterval(config_.default_resolution, timestamp_scale))
    , limit(config_.limit)
    , node_evaluation_range_getter(promql_tree_, config_)
{
}

}
