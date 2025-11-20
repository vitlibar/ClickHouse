#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Applies a subquery, for example
/// <expression>[1h:5m]
SQLQueryPiece subquery(SQLQueryPiece && expression, const PrometheusQueryTree::Node * promql_node, ConverterContext & context);

}
