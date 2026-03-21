#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Specifies which tags should be used to build the result of a binary operator on instant vectors
/// in case there are no grouping modifiers (i.e. neither group_left nor group_right is used).
enum class BinaryOperatorNoGroupingTagsSource
{
    /// Tags on which we join the left and right sides.
    /// Used by arithmetic operators (`+`, `-`, `*`, `/`, etc.) and comparison operators with the `bool` modifier.
    On,

    /// Tags from the left side.
    /// Used by comparison operators without the `bool` modifier (filter mode),
    /// where the result keeps the labels of the left-hand side.
    Left,

    /// There is no `Right` constant here because no operators need it.
};

/// Applies a simple binary operator (arithmetic or comparison) to two instant vectors or scalars.
/// The actual operation is provided via `apply_function_to_ast`, which receives two AST nodes
/// (left and right values) and returns the combined AST.
/// If at least one operand is scalar, the operation is applied element-wise without joining.
/// If both operands are instant vectors, they are joined on their label sets (respecting
/// `on()`/`ignoring()` and `group_left`/`group_right` modifiers from `operator_node`).
/// If `drop_metric_name` is true, the `__name__` label is removed from the result.
/// `no_grouping_tags_source` controls which side's tags become the result group when
/// neither `group_left` nor `group_right` is specified.
SQLQueryPiece applySimpleBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    BinaryOperatorNoGroupingTagsSource no_grouping_tags_source);

}
