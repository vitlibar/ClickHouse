#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathSimpleFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SimpleFunctionArgumentHelper.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <boost/math/special_functions/sign.hpp>
#include <numbers>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a math function.
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"abs",   {"abs"}},
            {"sgn",   {"sign"}},
            {"floor", {"floor"}},
            {"ceil",  {"ceil"}},
            {"sqrt",  {"sqrt"}},
            {"exp",   {"exp"}},
            {"ln",    {"log"}},
            {"log2",  {"log2"}},
            {"log10", {"log10"}},
            {"rad",   {"radians"}},
            {"deg",   {"degrees"}},
            {"sin",   {"sin"}},
            {"cos",   {"cos"}},
            {"tan",   {"tan"}},
            {"asin",  {"asin"}},
            {"acos",  {"acos"}},
            {"atan",  {"atan"}},
            {"sinh",  {"sinh"}},
            {"cosh",  {"cosh"}},
            {"tanh",  {"tanh"}},
            {"asinh", {"asinh"}},
            {"acosh", {"acosh"}},
            {"atanh", {"atanh"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isMathSimpleFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyMathSimpleFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);
    auto & argument = arguments[0];

    /// If the argument is empty then the result is also empty.
    if (argument.store_method == StoreMethod::EMPTY)
    {
        return SQLQueryPiece{function_node, function_node->result_type, StoreMethod::EMPTY};
    }

    SimpleFunctionArgumentHelper arg_helper{0, std::move(argument), context};
    auto result_store_method = getResultStoreMethod(arg_helper);

    SelectQueryBuilder builder;

    if (result_store_method == StoreMethod::VECTOR_GRID)
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

    auto transform_ast = [&](ASTPtr x) -> ASTPtr { return makeASTFunction(impl_info->ch_function_name, x); };

    builder.select_list.push_back(makeExpressionToEvaluateSimpleFunction(transform_ast, arg_helper));

    builder.select_list.back()->setAlias((result_store_method == StoreMethod::SINGLE_SCALAR) ? ColumnNames::Value : ColumnNames::Values);

    builder.from_table = arg_helper.table_to_select_from;

    SQLQueryPiece res{function_node, function_node->result_type, result_store_method};

    res.select_query = builder.getSelectQuery();
    res.start_time = arg_helper.start_time;
    res.end_time = arg_helper.end_time;
    res.step = arg_helper.step;

    return dropMetricName(std::move(res), context);
}

}
