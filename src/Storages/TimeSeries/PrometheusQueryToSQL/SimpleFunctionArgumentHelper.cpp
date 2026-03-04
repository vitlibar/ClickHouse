#include <Storages/TimeSeries/PrometheusQueryToSQL/SimpleFunctionArgumentHelper.h>

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

SimpleFunctionArgumentHelper::SimpleFunctionArgumentHelper(size_t argument_index_, SQLQueryPiece && argument, ConverterContext & context)
    : argument_index(argument_index_)
    , store_method(argument.store_method)
    , metric_name_dropped(argument.metric_name_dropped)
    , start_time(argument.start_time)
    , end_time(argument.end_time)
    , step(argument.step)
{
    switch (argument.store_method)
    {
        case StoreMethod::CONST_SCALAR:
        {
            ast = timeSeriesScalarToAST(argument.scalar_value, context.scalar_data_type);
            metric_name_dropped = true;
            return;
        }

        case StoreMethod::SINGLE_SCALAR:
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::SCALAR});
            String subquery_name = context.subqueries.back().name;
            ast = make_intrusive<ASTIdentifier>(subquery_name);
            metric_name_dropped = true;
            return;
        }

        case StoreMethod::SCALAR_GRID:
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::SCALAR});
            String subquery_name = context.subqueries.back().name;
            ast = make_intrusive<ASTIdentifier>(getIteratorNameInArrayMap(argument_index));
            array_map_lambda_arg = ast->clone();
            array_map_source_array = make_intrusive<ASTIdentifier>(subquery_name);
            metric_name_dropped = true;
            return;
        }

        case StoreMethod::VECTOR_GRID:
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            String subquery_name = context.subqueries.back().name;
            ast = make_intrusive<ASTIdentifier>(getIteratorNameInArrayMap(argument_index));
            array_map_lambda_arg = ast->clone();
            array_map_source_array = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            chassert(table_to_select_from.empty());
            table_to_select_from = subquery_name;
            return;
        }

        case StoreMethod::EMPTY:
        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "SimpleFunctionArgumentHelper can't handle {} because of its store method {}",
                            getPromQLText(argument, context), argument.store_method);
        }
    }

    UNREACHABLE();
}


ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr)> & apply_function_to_ast,
                                              const SimpleFunctionArgumentHelper & argument)
{
    bool need_array_map = (argument.array_map_source_array != nullptr);

    if (need_array_map)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", makeASTFunction("tuple", argument.array_map_lambda_arg), apply_function_to_ast(argument.ast)),
            argument.array_map_source_array);
    }
    else
    {
        return apply_function_to_ast(argument.ast);
    }
}


ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr, ASTPtr)> & apply_function_to_ast,
                                              const SimpleFunctionArgumentHelper & argument1,
                                              const SimpleFunctionArgumentHelper & argument2)
{
    bool need_array_map = (argument1.array_map_source_array != nullptr) || (argument2.array_map_source_array != nullptr);

    if (need_array_map)
    {
        auto tuple = makeASTFunction("tuple");
        if (argument1.array_map_lambda_arg)
            tuple->arguments->children.push_back(argument1.array_map_lambda_arg);
        if (argument2.array_map_lambda_arg)
            tuple->arguments->children.push_back(argument2.array_map_lambda_arg);
        auto array_map_function = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", tuple, apply_function_to_ast(argument1.ast, argument2.ast)));
        if (argument1.array_map_source_array)
            array_map_function->arguments->children.push_back(argument1.array_map_source_array);
        if (argument2.array_map_source_array)
            array_map_function->arguments->children.push_back(argument2.array_map_source_array);
        return array_map_function;
    }
    else
    {
        return apply_function_to_ast(argument1.ast, argument2.ast);
    }
}


ASTPtr makeExpressionToEvaluateSimpleFunction(const std::function<ASTPtr(ASTPtr, ASTPtr, ASTPtr)> & apply_function_to_ast,
                                              const SimpleFunctionArgumentHelper & argument1,
                                              const SimpleFunctionArgumentHelper & argument2,
                                              const SimpleFunctionArgumentHelper & argument3)
{
    bool need_array_map = (argument1.array_map_source_array != nullptr) || (argument2.array_map_source_array != nullptr) || (argument3.array_map_source_array != nullptr);

    if (need_array_map)
    {
        auto tuple = makeASTFunction("tuple");
        if (argument1.array_map_lambda_arg)
            tuple->arguments->children.push_back(argument1.array_map_lambda_arg);
        if (argument2.array_map_lambda_arg)
            tuple->arguments->children.push_back(argument2.array_map_lambda_arg);
        if (argument3.array_map_lambda_arg)
            tuple->arguments->children.push_back(argument3.array_map_lambda_arg);
        auto array_map_function = makeASTFunction(
            "arrayMap",
            makeASTFunction("lambda", tuple, apply_function_to_ast(argument1.ast, argument2.ast, argument3.ast)));
        if (argument1.array_map_source_array)
            array_map_function->arguments->children.push_back(argument1.array_map_source_array);
        if (argument2.array_map_source_array)
            array_map_function->arguments->children.push_back(argument2.array_map_source_array);
        if (argument3.array_map_source_array)
            array_map_function->arguments->children.push_back(argument3.array_map_source_array);
        return array_map_function;
    }
    else
    {
        return apply_function_to_ast(argument1.ast, argument2.ast, argument3.ast);
    }
}


StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument)
{
    if (argument.store_method == StoreMethod::CONST_SCALAR)
        return StoreMethod::SINGLE_SCALAR;

    return argument.store_method;
}


StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2)
{
    if (argument1.store_method == StoreMethod::VECTOR_GRID || argument2.store_method == StoreMethod::VECTOR_GRID)
        return StoreMethod::VECTOR_GRID;

    if (argument1.store_method == StoreMethod::SCALAR_GRID || argument2.store_method == StoreMethod::SCALAR_GRID)
        return StoreMethod::SCALAR_GRID;

    return StoreMethod::SINGLE_SCALAR;
}


StoreMethod getResultStoreMethod(const SimpleFunctionArgumentHelper & argument1, const SimpleFunctionArgumentHelper & argument2, const SimpleFunctionArgumentHelper & argument3)
{
    if (argument1.store_method == StoreMethod::VECTOR_GRID || argument2.store_method == StoreMethod::VECTOR_GRID || argument3.store_method == StoreMethod::VECTOR_GRID)
        return StoreMethod::VECTOR_GRID;

    if (argument1.store_method == StoreMethod::SCALAR_GRID || argument2.store_method == StoreMethod::SCALAR_GRID || argument3.store_method == StoreMethod::SCALAR_GRID)
        return StoreMethod::SCALAR_GRID;

    return StoreMethod::SINGLE_SCALAR;
}

}
