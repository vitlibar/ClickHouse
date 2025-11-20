#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>

#include <Core/DecimalFunctions.h>
#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTimestamp.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies an offset for the evaluation time: <expression> offset 1d
    SQLQueryPiece offsetEvaluationTime(
        SQLQueryPiece && expression,
        const DecimalField<Decimal64> & offset,
        const PrometheusQueryTree::Node * promql_node,
        ConverterContext & context)
    {
        switch (expression.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::GRID:
            {
                SQLQueryPiece res = std::move(expression);
                res.start_time = addTimeSeriesInterval(res.start_time, offset);
                res.end_time = addTimeSeriesInterval(res.end_time, offset);
                return res;
            }

            case StoreMethod::RAW_DATA:
            {
                SQLQueryPiece res{promql_node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                UInt32 max_scale = std::max<UInt32>((context.timestamp_scale + 2) / 3 * 3, 9);
                Int64 scaled_offset = DecimalUtils::convertTo<Decimal64>(max_scale, offset.getValue(), offset.getScale());

                static const std::string_view interval_functions[] = {"toIntervalSecond", "toIntervalMillisecond", "toIntervalMicrosecond", "toIntervalNanosecond"};
                std::string_view interval_function = interval_functions[max_scale / 3];

                ASTPtr new_timestamp = makeASTFunction(
                    "plus",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                    makeASTFunction(interval_function, std::make_shared<ASTLiteral>(scaled_offset)));

                if (context.timestamp_scale < max_scale)
                    new_timestamp = makeASTFunction("toDateTime64", new_timestamp, std::make_shared<ASTLiteral>(context.timestamp_scale));

                params.select_list.push_back(new_timestamp);
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));
                return res;
            }
        }

        UNREACHABLE();
    }

    /// Applies setting a fixed evaluation time: <expression> @ 1609746000
    SQLQueryPiece setEvaluationTime(SQLQueryPiece && expression, const PrometheusQueryTree::Node * promql_node, ConverterContext & context)
    {
        auto evaluation_range = context.node_evaluation_range_getter.get(promql_node);

        switch (expression.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::GRID:
            {
                SQLQueryPiece res = std::move(expression);
                res.start_time = evaluation_range.start_time;
                res.end_time = evaluation_range.end_time;
                res.step = evaluation_range.step;
                return res;
            }

            case StoreMethod::RAW_DATA:
            {
                SQLQueryPiece res{promql_node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                params.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeSeriesTimestampToAST(evaluation_range.start_time, context.timestamp_type),
                        timeSeriesTimestampToAST(evaluation_range.end_time, context.timestamp_type),
                        timeSeriesIntervalToAST(evaluation_range.step))));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));
                return res;
            }
        }

        UNREACHABLE();
    }
}

SQLQueryPiece modifyEvaluationTime(
    SQLQueryPiece && expression,
    const PrometheusQueryTree::At * at_node,
    ConverterContext & context)
{
    if (at_node->getAt())
    {
        /// Set fixed evaluation time.
        return setEvaluationTime(std::move(expression), at_node, context);
    }
    else if (const auto * offset = at_node->getOffset())
    {
        /// Add offset to the evaluation time.
        auto offset_value = nodeToInterval(offset, context.timestamp_scale);
        return offsetEvaluationTime(std::move(expression), offset_value, at_node, context);
    }
    else
    {
        return expression;
    }
}

}
