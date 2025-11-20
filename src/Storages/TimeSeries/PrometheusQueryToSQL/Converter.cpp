#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSelector.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyRangeVectorFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/subquery.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece visitNode(const PrometheusQueryTree::Node * node, ConverterContext & context)
    {
        switch (node->node_type)
        {
            case PrometheusQueryTree::NodeType::InstantSelector:
            {
                const auto * instant_selector = static_cast<const PrometheusQueryTree::InstantSelector *>(node);
                return makeSelector(instant_selector, context);
            }

            case PrometheusQueryTree::NodeType::RangeSelector:
            {
                const auto * range_selector = static_cast<const PrometheusQueryTree::RangeSelector *>(node);
                return makeSelector(range_selector, context);
            }

            case PrometheusQueryTree::NodeType::Function:
            {
                const auto * function = static_cast<const PrometheusQueryTree::Function *>(node);
                std::vector<SQLQueryPiece> arguments;
                for (const auto * arg_node : function->getArguments())
                {
                    arguments.push_back(visitNode(arg_node, context));
                }

                if (isRangeVectorFunction(function->function_name))
                    return applyRangeVectorFunction(function->function_name, std::move(arguments[0]), node, context);
                else
                    throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Unknown function with name {}", function->function_name);
            }

            case PrometheusQueryTree::NodeType::At:
            {
                const auto * at_node = static_cast<const PrometheusQueryTree::At *>(node);
                SQLQueryPiece expression = visitNode(at_node->getExpression(), context);
                return modifyEvaluationTime(std::move(expression), at_node, context);
            }

            case PrometheusQueryTree::NodeType::Subquery:
            {
                const auto * subquery_node = static_cast<const PrometheusQueryTree::Subquery *>(node);
                SQLQueryPiece expression = visitNode(subquery_node->getExpression(), context);
                return subquery(std::move(expression), node, context);
            }

            default:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Prometheus query node type {} is not implemented", node->node_type);
            }
        }
    }
}


Converter::Converter(PrometheusQueryTree promql_tree_, ConverterConfig config_)
    : promql_tree(std::move(promql_tree_))
    , config(std::move(config_))
{
    if (!config.evaluation_time.isNull())
    {
        result_type = promql_tree.getResultType();
    }
    else if (!config.evaluation_range.isNull())
    {
        checkPrometheusQueryAllowsEvaluationRange(promql_tree);
        result_type = ResultType::RANGE_VECTOR;
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");
    }
}


ColumnsDescription Converter::getResultColumns() const
{
    return DB::PrometheusQueryToSQL::getResultColumns(result_type, config.timestamp_type, config.scalar_type);
}


ASTPtr Converter::getSQL() const
{
    ConverterContext context{promql_tree, config};
    auto query_piece = visitNode(promql_tree.getRoot(), context);
    if (!config.evaluation_range.isNull())
        query_piece.type = ResultType::RANGE_VECTOR;
    return finalizeSQL(std::move(query_piece), context);
}

}
