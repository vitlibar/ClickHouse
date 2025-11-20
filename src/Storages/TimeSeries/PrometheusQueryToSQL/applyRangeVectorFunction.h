#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;
struct NodeEvaluationRange;

/// Returns true if it's the name of a prometheus function taking a range vector.
bool isRangeVectorFunction(const String & promql_function_name);

/// Applies a prometheus function taking a range vector to a SQL query built to calculate its argument.
/// Supports various functions, for example rate(), last_over_time().
SQLQueryPiece applyRangeVectorFunction(const String & promql_function_name, SQLQueryPiece && argument,
                                       const PrometheusQueryTree::Node * promql_node, ConverterContext & context);

}
