#if 0
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"+",     {"plus"}},
            {"-",     {"minus"}},
            {"*",     {"multiply"}},
            {"/",     {"divide"}},
            {"%",     {"modulo"}},
            {"^",     {"pow"}},
            {"atan2", {"atan2"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isMathBinaryOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyMathBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    return applyMathLikeBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context, impl_info->ch_function_name)
}

SQLQueryPiece applyMathLikeBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::string_view ch_function_name)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
    {
        /// If one of the arguments has no data, the result also has no data.
        return SQLQueryPiece{operator_node, operator_node->type, StoreMethod::EMPTY};
    }

}

}
#endif
