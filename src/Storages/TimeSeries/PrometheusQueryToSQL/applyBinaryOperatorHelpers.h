#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns an AST to evaluate the `join_group` column to join the sides of a binary operator on instant vectors.
ASTPtr makeExpressionForJoinGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && group,
    bool metric_name_dropped_from_group,
    bool * metric_name_dropped_from_join_group = nullptr);

/// Returns an AST to evaluate the group which will be set for the result of a binary operator on instant vectors.
ASTPtr makeExpressionForResultGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && left_argument_group,
    ASTPtr && right_argument_group,
    ASTPtr && join_group,
    bool metric_name_dropped_from_left,
    bool metric_name_dropped_from_right,
    bool metric_name_dropped_from_join_group,
    bool * metric_name_dropped_from_result = nullptr);

}
