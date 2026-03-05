#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunctionHelper.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    String getIteratorNameInArrayMap(size_t argument_index)
    {
        chassert(argument_index <= 2);
        String str;
        str.push_back(static_cast<char>('x' + argument_index));
        return str;
    }
}

SQLQueryPiece applySimpleFunctionHelper(
    PQT::Node * node,
    ConverterContext & context,
    const std::function<ASTPtr(ASTs)> & apply_function_to_ast,
    std::vector<SQLQueryPiece> && arguments)
{
    SQLQueryPiece res{node, node->result_type, StoreMethod::EMPTY};

    ASTs asts;
    ASTs array_map_source_arrays;
    ASTs array_map_lambda_args;
    String table_to_select_from;

    for (size_t i = 0; i != arguments.size(); ++i)
    {
        auto & argument = arguments[i];

        switch (argument.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// If the argument is empty then the result is also empty.
                return SQLQueryPiece{node, node->result_type, StoreMethod::EMPTY};
            }

            case StoreMethod::CONST_SCALAR:
            {
                asts.push_back(timeSeriesScalarToAST(argument.scalar_value, context.scalar_data_type));

                if (res.store_method != StoreMethod::SCALAR_GRID && res.store_method != StoreMethod::VECTOR_GRID)
                    res.store_method = StoreMethod::SINGLE_SCALAR;
                
                break;
            }

            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::SCALAR});
                String subquery_name = context.subqueries.back().name;

                asts.push_back(make_intrusive<ASTIdentifier>(subquery_name));

                if (res.store_method != StoreMethod::SCALAR_GRID && res.store_method != StoreMethod::VECTOR_GRID)
                    res.store_method = StoreMethod::SINGLE_SCALAR;

                break;
            }

            case StoreMethod::SCALAR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::SCALAR});
                String subquery_name = context.subqueries.back().name;

                String iterator_name = getIteratorNameInArrayMap(argument_index);
                asts.push_back(make_intrusive<ASTIdentifier>(iterator_name));
                array_map_lambda_args.push_back(make_intrusive<ASTIdentifier>(iterator_name));
                array_map_source_arrays.push_back(make_intrusive<ASTIdentifier>(subquery_name));

                if (res.store_method != StoreMethod::VECTOR_GRID)
                    res.store_method = StoreMethod::SCALAR_GRID;

                break;
            }

            case StoreMethod::VECTOR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
                String subquery_name = context.subqueries.back().name;

                String iterator_name = getIteratorNameInArrayMap(argument_index);
                asts.push_back(make_intrusive<ASTIdentifier>(iterator_name));
                array_map_lambda_args.push_back(make_intrusive<ASTIdentifier>(iterator_name));
                array_map_source_arrays.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                chassert(table_to_select_from.empty());
                table_to_select_from = subquery_name;

                res.store_method = StoreMethod::VECTOR_GRID;
                res.metric_name_dropped = argument.metric_name_dropped;

                break;
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "applySimpleFunctionHelper can't handle {} because of its store method {}",
                                getPromQLText(argument, context), argument.store_method);
            }
        }

        res.start_time = argument.start_time;
        res.end_time = argument.end_time;
        res.step = argument.step;
    }

    SelectQueryBuilder builder;

    if (res.store_method == StoreMethod::VECTOR_GRID)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    ASTPtr new_value;

    bool need_array_map = !array_map_source_arrays.empty();
    if (need_array_map)
    {
        auto tuple = makeASTFunction("tuple");
        tuple->arguments->children = std::move(array_map_lambda_args);

        auto array_map_function = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", tuple, apply_function_to_ast(std::move(asts))));

        array_map_function->arguments->children.insert(array_map_function->arguments->children.end(),
            array_map_source_arrays.begin(), array_map_source_arrays.end());

        new_value = array_map_function;
    }
    else
    {
        new_value = apply_function_to_ast(std::move(asts));
    }

    builder.select_list.push_back(std::move(new_value));
    builder.select_list.back()->setAlias((res.store_method == StoreMethod::SINGLE_SCALAR) ? ColumnNames::Value : ColumnNames::Values);

    builder.from_table = table_to_select_from;

    res.select_query = builder.getSelectQuery();

    return res;
}

}
