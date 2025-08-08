#pragma once

#include <Core/Field.h>


namespace DB
{

struct PrometheusQueryEvaluationRange
{
    Field start_time;
    Field end_time;
    Field step;
};

}
