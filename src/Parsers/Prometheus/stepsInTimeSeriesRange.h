#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

size_t stepsInTimeSeriesRange(DateTime64 start_time, DateTime64 end_time, Decimal64 step);

}
