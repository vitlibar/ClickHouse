#pragma once

#include <DataTypes/IDataType.h>
#include <Interpreters/StorageID.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{

struct PrometheusQueryEvaluationSettings
{
    StorageID time_series_storage_id = StorageID::createEmpty();
    DataTypePtr timestamp_data_type;
    UInt32 timestamp_scale = 0;
    DataTypePtr value_data_type;

    /// `evaluation_time` sets a specific time when the prometheus query is evaluated,
    /// `evaluation_range` sets a range of such times.
    /// If neither `evaluation_time` nor `evaluation_range` is set then the current time is used.
    std::optional<DateTime64> evaluation_time;
    std::optional<PrometheusQueryEvaluationRange> evaluation_range;

    /// The lookback period. If not set then 5 minutes are used by default.
    std::optional<Decimal64> lookback_delta;

    /// The default subquery resolution. If not set then 15 seconds are used by default.
    std::optional<Decimal64> default_resolution;
};

}
