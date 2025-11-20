#pragma once

#include <Core/Field.h>


namespace DB
{
class PrometheusQueryTree;

/// Specifies that a prometheus query should be evaluated starting with `start_time` and ending with `end_time` with a specified `step`.
struct PrometheusQueryEvaluationRange
{
    Field start_time;
    Field end_time;
    Field step;

    bool isNull() const { return start_time.isNull(); }
};

/// Throws an exception if the specified prometheus query doesn't allow evaluating over a range.
void checkPrometheusQueryAllowsEvaluationRange(const PrometheusQueryTree & promql_tree);

}
