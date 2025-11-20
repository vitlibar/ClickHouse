#pragma once

#include <Core/Field.h>
#include <Interpreters/StorageID.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{
    class Field;
}


namespace DB::PrometheusQueryToSQL
{

struct ConverterConfig
{
    StorageID time_series_storage_id = StorageID::createEmpty();
    DataTypePtr timestamp_type;
    DataTypePtr scalar_type;

    /// Either `evaluation_time` or `evaluation_range` should be set.
    /// Evaluate a prometheus query at a specified evaluation time.
    Field evaluation_time;
    /// Evaluate a prometheus query over a range of time.
    PrometheusQueryEvaluationRange evaluation_range;

    Field lookback_delta{5*60};   /// 5 minutes
    Field default_resolution{15}; /// 15 seconds
    std::optional<size_t> limit;
};

}
