#pragma once

#include <base/Decimal.h>


namespace DB
{

/// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time` with a specified `step`.
struct PrometheusQueryEvaluationRange
{
    DateTime64 start_time;
    DateTime64 end_time;
    Decimal64 step;
};

}
