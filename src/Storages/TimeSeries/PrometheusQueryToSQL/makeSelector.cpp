#include <Storages/TimeSeries/PrometheusQueryToSQL/makeSelector.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyRangeVectorFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    SQLQueryPiece makeRangeSelector(std::string_view instant_selector_text,
                                    const PrometheusQueryTree::Node * promql_node,
                                    ConverterContext & context)
    {
        SQLQueryPiece res{promql_node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

        SelectQueryParams params;
        params.select_list.push_back(makeASTFunction("timeSeriesIdToGroup", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID)));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);

        params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

        auto evaluation_range = context.node_evaluation_range_getter.get(promql_node);

        params.from_table_function = makeASTFunction("timeSeriesSelectorToGrid",
            std::make_shared<ASTLiteral>(context.time_series_storage_id.getDatabaseName()),
            std::make_shared<ASTLiteral>(context.time_series_storage_id.getTableName()),
            std::make_shared<ASTLiteral>(String{instant_selector_text}),
            timeSeriesTimestampToAST(evaluation_range.start_time, context.timestamp_type),
            timeSeriesTimestampToAST(evaluation_range.end_time, context.timestamp_type),
            timeSeriesIntervalToAST(evaluation_range.step),
            timeSeriesIntervalToAST(evaluation_range.window));

        res.select_query = buildSelectQuery(std::move(params));
        return res;
    }
}


SQLQueryPiece makeSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context)
{
    auto instant_selector_text = context.promql_tree.getQuery(instant_selector_node);
    auto range_selector = makeRangeSelector(instant_selector_text, instant_selector_node, context);
    return applyRangeVectorFunction("last_over_time", std::move(range_selector), instant_selector_node, context);
}


SQLQueryPiece makeSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context)
{
    auto instant_selector_text = context.promql_tree.getQuery(range_selector_node->getInstantSelector());
    return makeRangeSelector(instant_selector_text, range_selector_node, context);
}

}
