#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Increases a timestamp to make it divisible by `step`. 
TimestampType alignTimestampUp(TimestampType timestamp, DurationType step);

/// Decreases a timestamp to make it divisible by `step`. 
TimestampType alignTimestampDown(TimestampType timestamp, DurationType step);

}
