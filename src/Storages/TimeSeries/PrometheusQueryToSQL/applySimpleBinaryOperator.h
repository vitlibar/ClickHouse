#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies a simple binary operator (arithmetic or comparison) to two instant vectors or scalars.
/// The actual operation is provided via `apply_function_to_ast`, which receives two AST nodes
/// (left and right values) and returns the combined AST.
/// If at least one operand is scalar, the operation is applied element-wise without joining.
/// If both operands are instant vectors, they are joined on their label sets (respecting
/// `on()`/`ignoring()` and `group_left`/`group_right` modifiers from `operator_node`).
/// If `drop_metric_name` is true, the `__name__` label is removed from the result unless
/// it's specified among the extra labels in group_left(<extra_labels>) or group_right(<extra_labels>).
SQLQueryPiece applySimpleBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name);

}
