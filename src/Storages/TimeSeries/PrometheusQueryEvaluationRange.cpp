#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void checkPrometheusQueryAllowsEvaluationRange(const PrometheusQueryTree & promql_tree)
{
    if ((promql_tree.getResultType() == PrometheusQueryResultType::SCALAR)
        || (promql_tree.getResultType() == PrometheusQueryResultType::INSTANT_VECTOR))
        return;

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Invalid expression type {} for range query, must be {} or {}",
        promql_tree.getResultType(),
        PrometheusQueryResultType::SCALAR,
        PrometheusQueryResultType::INSTANT_VECTOR);
}

}
