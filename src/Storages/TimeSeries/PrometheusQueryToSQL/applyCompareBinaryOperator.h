#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether a specified string is the name of a prometheus comparison operator:
/// '==', '!=', '>', '<', '>=', '<='
bool isCompareBinaryOperator(std::string_view operator_name);

/// Applies a prometheus compare operator.
SQLQueryPiece applyCompareBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
