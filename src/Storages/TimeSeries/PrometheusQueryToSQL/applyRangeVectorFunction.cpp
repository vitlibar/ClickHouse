#include <Storages/TimeSeries/PrometheusQueryToSQL/applyRangeVectorFunction.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    String getSQLFunctionName(const String & promql_function_name)
    {
        if (promql_function_name == "rate")
            return "timeSeriesRateToGrid";
        else if (promql_function_name == "irate")
            return "timeSeriesInstantRateToGrid";
        else if (promql_function_name == "delta")
            return "timeSeriesDeltaToGrid";
        else if (promql_function_name == "idelta")
            return "timeSeriesInstantDeltaToGrid";
        else if (promql_function_name == "idelta")
            return "timeSeriesInstantDeltaToGrid";
        else if (promql_function_name == "last_over_time")
            return "timeSeriesLastToGrid";
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Function {} is not implemented", promql_function_name);
    }


    void checkArgumentTypes(
        const String & promql_function_name,
        const SQLQueryPiece & argument,
        const PrometheusQueryTree & promql_tree,
        const PrometheusQueryTree::Node * promql_node)
    {
        if (argument.type != ResultType::RANGE_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Function {} expects {}, but argument {} has type {}",
                            promql_function_name, ResultType::RANGE_VECTOR, promql_tree.getQuery(promql_node), argument.type);
        }
    }


    void addParametersToAggregateFunction(
        ASTFunction & function,
        DecimalField<DateTime64> start_time,
        DecimalField<DateTime64> end_time,
        DecimalField<Decimal64> step,
        DecimalField<Decimal64> window)
    {
        if (!function.parameters)
        {
            function.parameters = std::make_shared<ASTExpressionList>();
            function.children.push_back(function.parameters);
        }
        function.parameters->children.push_back(std::make_shared<ASTLiteral>(start_time));
        function.parameters->children.push_back(std::make_shared<ASTLiteral>(end_time));
        function.parameters->children.push_back(std::make_shared<ASTLiteral>(step));
        function.parameters->children.push_back(std::make_shared<ASTLiteral>(window));
    }
}


bool isRangeVectorFunction(const String & promql_function_name)
{
    static const std::unordered_set<std::string_view> all_range_vector_functions = {
        "rate", "irate", "delta", "idelta", "last_over_time"
    };
    return all_range_vector_functions.contains(promql_function_name);
}


SQLQueryPiece applyRangeVectorFunction(
    const String & promql_function_name,
    SQLQueryPiece && argument,
    const PrometheusQueryTree::Node * promql_node,
    ConverterContext & context)
{
    checkArgumentTypes(promql_function_name, argument, context.promql_tree, promql_node);

    auto evaluation_range = context.node_evaluation_range_getter.get(promql_node);
    auto start_time = evaluation_range.start_time;
    auto end_time = evaluation_range.end_time;
    auto step = evaluation_range.step;
    auto window = evaluation_range.window;

    if (start_time > end_time)
    {
        /// Evaluation range is empty.

        /// SELECT arrayJoin([]::Array(UInt64)) AS group,
        ///        defaultValueOfTypeName(Array(Nullable(scalar_type))) AS values
        struct SelectQueryParams params;

        params.select_list.push_back(makeASTFunction(
            "arrayJoin", makeASTFunction("CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(UInt64)"))));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

        params.select_list.push_back(makeASTFunction(
            "defaultValueOfTypeName",
            std::make_shared<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_type->getName()))));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

        SQLQueryPiece res{promql_node, ResultType::INSTANT_VECTOR, StoreMethod::GRID};
        res.start_time = end_time;
        res.end_time = end_time;
        res.select_query = buildSelectQuery(std::move(params));

        return res;
    }

    switch (argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            /// SELECT 0 AS group,
            ///        <aggregate_function>(timeSeriesRange(start_time::timestamp_type, end_time::timestamp_type, step),
            ///                             replicate(number_of_steps, [scalar_value]::Array(scalar_type))
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTLiteral>(0u));
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);

            auto aggregate_function = makeASTFunction(
                getSQLFunctionName(promql_function_name),
                makeASTFunction(
                    "timeSeriesRange",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_type),
                    timeSeriesIntervalToAST(argument.step)),
                makeASTFunction(
                    "replicate",
                    std::make_shared<ASTLiteral>(getNumberOfTimeSeriesSteps(argument.start_time, argument.end_time, argument.step)),
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTLiteral>(Array{argument.scalar_value}),
                        std::make_shared<ASTLiteral>(fmt::format("Array({})", context.scalar_type->getName())))));

            addParametersToAggregateFunction(*aggregate_function, start_time, end_time, step, window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

            SQLQueryPiece res{promql_node, ResultType::INSTANT_VECTOR, StoreMethod::GRID};

            res.select_query = buildSelectQuery(std::move(params));
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            return res;
        }

        case StoreMethod::GRID:
        {
            /// SELECT group,
            ///        <aggregate_function>(timeSeriesRange(start_time::timestamp_type, end_time::timestamp_type, step),
            ///                             values)
            /// GROUP BY group
            SelectQueryParams params;
            params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

            auto aggregate_function = makeASTFunction(
                getSQLFunctionName(promql_function_name),
                makeASTFunction(
                    "timeSeriesRange",
                    timeSeriesTimestampToAST(argument.start_time, context.timestamp_type),
                    timeSeriesTimestampToAST(argument.end_time, context.timestamp_type),
                    timeSeriesIntervalToAST(argument.step)),
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values));

            addParametersToAggregateFunction(*aggregate_function, start_time, end_time, step, window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(SQLSubquery{subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            params.from_subquery = subqueries.back().name;

            SQLQueryPiece res{promql_node, ResultType::INSTANT_VECTOR, StoreMethod::GRID};

            res.select_query = buildSelectQuery(std::move(params));
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            return res;
        }

        case StoreMethod::RAW_DATA:
        {
            /// SELECT group,
            ///        <aggregate_function>(timestamps, values)
            /// GROUP BY group
            SelectQueryParams params;

            params.select_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

            auto aggregate_function = makeASTFunction(
                getSQLFunctionName(promql_function_name),
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));

            addParametersToAggregateFunction(*aggregate_function, start_time, end_time, step, window);

            params.select_list.push_back(aggregate_function);
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

            params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

            auto & subqueries = context.subqueries;
            subqueries.emplace_back(subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE);
            params.from_subquery = subqueries.back().name;

            SQLQueryPiece res{promql_node, ResultType::INSTANT_VECTOR, StoreMethod::GRID};

            res.select_query = buildSelectQuery(std::move(params));
            res.start_time = start_time;
            res.end_time = end_time;
            res.step = step;

            return res;
        }

        case StoreMethod::CONST_STRING:
        {
            throwStoreMethodIsNotSupported(argument, context.promql_tree);
        }
    }

    UNREACHABLE();
}

}
