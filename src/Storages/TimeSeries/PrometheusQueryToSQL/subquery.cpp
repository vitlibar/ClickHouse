#include <Storages/TimeSeries/PrometheusQueryToSQL/subquery.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkExpressionType(
        const SQLQueryPiece & expression,
        const PrometheusQueryTree & promql_tree,
        const PrometheusQueryTree::Node * promql_node)
    {
        if (expression.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Expression {} has type {} and can't be used in a subquery",
                            promql_tree.getQuery(promql_node), expression.type);
        }
    }
}


SQLQueryPiece subquery(SQLQueryPiece && expression, const PrometheusQueryTree::Node * promql_node, ConverterContext & context)
{
    checkExpressionType(expression, context.promql_tree, promql_node);

    expression.promql_node = promql_node;
    expression.type = ResultType::RANGE_VECTOR;
    return expression;
}

}
