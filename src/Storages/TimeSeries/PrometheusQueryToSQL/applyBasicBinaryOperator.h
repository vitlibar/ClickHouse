#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Specifies which tags should be used to build the result of a binary operator on instant vectors
/// in case there are no grouping modifiers (i.e. neither group_left nor group_right is used).
enum class BinaryOperatorNoGroupingTagsSource
{
    /// Tags from the left side.
    Left,

    /// Tags from the right side.
    Right,

    /// Tags on which we join the left and right sides.
    On,
};

SQLQueryPiece applyBasicBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    BinaryOperatorNoGroupingTagsSource no_grouping_tags_source);

}
