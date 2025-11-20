#include <Storages/TimeSeries/PrometheusQueryToSQL/modifyEvaluationTime.h>

#include <Core/DecimalFunctions.h>
#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/nodeToTime.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies an offset for the evaluation time: <expression> offset 1d
    SQLQueryPiece offsetEvaluationTime(
        const PrometheusQueryTree::At * at_node,
        SQLQueryPiece && expression,
        const DecimalField<Decimal64> & offset,
        ConverterContext & context)
    {
        switch (expression.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                SQLQueryPiece res = std::move(expression);
                res.promql_node = at_node;
                res.start_time = addTimeseriesDuration(res.start_time, offset);
                res.end_time = addTimeseriesDuration(res.end_time, offset);
                return res;
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT group, timestamp + INTERVAL X, value FROM <raw_data>
                SQLQueryPiece res{at_node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                /// Round up the scale to next number divisible by 3 but not greater than 9 (nanoseconds scale).
                UInt32 max_scale = std::max<UInt32>((context.max_time_scale + 2) / 3 * 3, 9);
                Int64 scaled_offset = DecimalUtils::convertTo<Decimal64>(max_scale, offset.getValue(), offset.getScale());

                static const std::string_view interval_functions[] = {"toIntervalSecond", "toIntervalMillisecond", "toIntervalMicrosecond", "toIntervalNanosecond"};
                std::string_view interval_function = interval_functions[max_scale / 3];

                ASTPtr new_timestamp = makeASTFunction(
                    "plus",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                    makeASTFunction(interval_function, std::make_shared<ASTLiteral>(scaled_offset)));

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
    SQLQueryPiece setEvaluationTime(const PrometheusQueryTree::At * at_node, SQLQueryPiece && expression, ConverterContext & context)
    {
        /// <expression> is expected to be calculated at a fixed evaluation time.
        checkStartTimeEqualsToEndTime(expression, context.promql_tree);

        auto evaluation_range = context.node_evaluation_range_getter.get(at_node);

        switch (expression.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            case StoreMethod::CONST_STRING:
            {
                SQLQueryPiece res = std::move(expression);
                res.promql_node = at_node;
                res.start_time = evaluation_range.start_time;
                res.end_time = evaluation_range.end_time;
                res.step = evaluation_range.step;
                return res;
            }

            case StoreMethod::SCALAR_GRID:
            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT arrayResize([], count_of_time_steps, values[1])) FROM <scalar_grid>
                /// SELECT group, arrayResize([], count_of_time_steps, values[1])) FROM <vector_grid>
                SQLQueryPiece res{at_node, expression.type, expression.store_method};

                SelectQueryParams params;
                if (expression.store_method == StoreMethod::VECTOR_GRID)
                    params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                params.select_list.push_back(makeASTFunction(
                    "arrayResize",
                    std::make_shared<ASTLiteral>(Array{}),
                    std::make_shared<ASTLiteral>(
                        countTimeseriesSteps(evaluation_range.start_time, evaluation_range.end_time, evaluation_range.step)),
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u))));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

                auto & subqueries = context.subqueries;
                subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(expression.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = subqueries.back().name;

                res.select_query = buildSelectQuery(std::move(params));
                return res;
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT group, arrayJoin(timeSeriesRange(start_time, end_time, step)), value
                SQLQueryPiece res{at_node, ResultType::RANGE_VECTOR, StoreMethod::RAW_DATA};

                SelectQueryParams params;
                params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                params.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeseriesTimeToAST(evaluation_range.start_time),
                        timeseriesTimeToAST(evaluation_range.end_time),
                        timeseriesDurationToAST(evaluation_range.step))));

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

SQLQueryPiece modifyEvaluationTime(const PrometheusQueryTree::At * at_node, SQLQueryPiece && expression, ConverterContext & context)
{
    if (at_node->getAt())
    {
        /// Set fixed evaluation time.
        return setEvaluationTime(at_node, std::move(expression), context);
    }
    else if (const auto * offset = at_node->getOffset())
    {
        /// Add offset to the evaluation time.
        auto offset_value = nodeToDuration(offset, context.max_time_scale);
        return offsetEvaluationTime(at_node, std::move(expression), offset_value, context);
    }
    else
    {
        return expression;
    }
}

}
