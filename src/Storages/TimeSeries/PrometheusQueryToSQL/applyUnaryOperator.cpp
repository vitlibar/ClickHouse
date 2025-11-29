#include <Storages/TimeSeries/PrometheusQueryToSQL/applyUnaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PrometheusQueryTree::UnaryOperator * operator_node,
        const SQLQueryPiece & argument,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;
        if (!(operator_name == "+" || operator_name == "-"))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Unknown unary operator with name {}", operator_name);
        }

        if (!(argument.type == ResultType::SCALAR || argument.type == ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Operator '{}' expects an argument of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLQuery(argument, context), argument.type);
        }
    }
}


SQLQueryPiece applyUnaryOperator(
    const PrometheusQueryTree::UnaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context)
{
    checkArgumentTypes(operator_node, argument, context);
    const auto & operator_name = operator_node->operator_name;

    switch (argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            SQLQueryPiece res{operator_node, argument.type, StoreMethod::CONST_SCALAR};
            res.start_time = argument.start_time;
            res.end_time = argument.end_time;
            res.step = argument.step;

            res.scalar_value = (operator_name == "-") ? -argument.scalar_value  : argument.scalar_value;

            return res;
        }

        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        {
            SQLQueryPiece res{operator_node, argument.type, argument.store_method};
            res.start_time = argument.start_time;
            res.end_time = argument.end_time;
            res.step = argument.step;

            if (operator_name != "-")
            {
                res.select_query = std::move(argument.select_query);
                return res;
            }

            /// For scalar grid:
            /// SELECT arrayMap(x -> -x, values) AS values
            /// FROM <scalar_grid>
            ///
            /// For vector grid:
            /// SELECT timeSeriesRemoveTagFromGroup(group, '__name__') AS group,
            ///        timeSeriesGroupValuesFromGridOrThrow(arrayMap(x -> -x, values)) AS values
            /// FROM <vector_grid>
            /// GROUP BY group

            SelectQueryParams params;
            if (argument.store_method == StoreMethod::VECTOR_GRID)
            {
                params.select_list.push_back(makeASTFunction(
                    "timeSeriesRemoveTag",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                    std::make_shared<ASTLiteral>("__name__")));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::Group);
            }

            auto transform = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", std::make_shared<ASTIdentifier>("x")),
                    makeASTFunction("negate", std::make_shared<ASTIdentifier>("x"))),
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values));

            if (argument.store_method == StoreMethod::VECTOR_GRID)
            {
                transform = makeASTFunction("timeSeriesGroupValuesFromGridOrThrow", transform);
            }

            params.select_list.push_back(transform);
            params.select_list.back()->setAlias(TimeSeriesColumnNames::Values);

            if (argument.store_method == StoreMethod::VECTOR_GRID)
            {
                params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));
            }

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            params.from_subquery = context.subqueries.back().name;

            res.select_query = buildSelectQuery(std::move(params));
            return res;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            throwStoreMethodIsNotSupported(argument, context);
        }
    }

    UNREACHABLE();
}

}
