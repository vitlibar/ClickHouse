#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector,
/// for example rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

SQLQueryPiece applyFunctionOverRange(
    const Node * node, const String & function_name, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
