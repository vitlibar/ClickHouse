#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Builds an AST expression for the `join_group` column, which is the matching key used to pair up series
/// from both sides of a binary operator. The expression is derived from `group` by applying the `on(tags)`
/// or `ignoring(tags)` matching rules — removing tags that should not participate in matching.
ASTPtr makeASTForBinaryOperatorJoinGroup(
    const PQT::BinaryOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped);

}
