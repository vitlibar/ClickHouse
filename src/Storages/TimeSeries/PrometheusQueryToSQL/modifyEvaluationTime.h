#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;
struct NodeEvaluationRange;

/// Applies an offset of the evaluation time, for example
/// <expression> offset 1d
/// or
/// <expression> @ 1609746000
SQLQueryPiece modifyEvaluationTime(
    SQLQueryPiece && expression,
    const PrometheusQueryTree::At * at_node,
    ConverterContext & context);

}
