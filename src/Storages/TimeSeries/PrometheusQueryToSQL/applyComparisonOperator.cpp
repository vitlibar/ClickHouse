#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBasicBinaryOperator.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(
        const PQT::BinaryOperator * operator_node,
        const SQLQueryPiece & left_argument,
        const SQLQueryPiece & right_argument,
        const ConverterContext & context)
    {
        std::string_view operator_name = operator_node->operator_name;

        if ((left_argument.type != ResultType::SCALAR) && (left_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(left_argument, context), left_argument.type);
        }

        if ((right_argument.type != ResultType::SCALAR) && (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' expects two arguments of type {} or {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR, ResultType::INSTANT_VECTOR,
                            getPromQLText(right_argument, context), right_argument.type);
        }

        if ((left_argument.type == ResultType::SCALAR) && (right_argument.type == ResultType::SCALAR) && !operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Comparison operator '{}' on scalars requires the bool modifier",
                            operator_name);
        }

        if ((left_argument.type != ResultType::INSTANT_VECTOR) || (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            if (operator_node->group_left || operator_node->group_right)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Binary operator '{}' with the group modifier expects two arguments of type {}, got {} and {}",
                                operator_name, ResultType::INSTANT_VECTOR, left_argument.type, right_argument.type);
            }
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"==", {"equals"}},
            {"!=", {"notEquals"}},
            {">",  {"greater"}},
            {"<",  {"less"}},
            {">=", {"greaterOrEquals"}},
            {"<=", {"lessOrEquals"}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isComparisonOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyComparisonOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    /// If the bool modifier is specified then the operator works like arithmetic operators.
    if (operator_node->bool_modifier)
    {
        auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
        {
            return timeSeriesScalarASTCast(
                makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y)), context.scalar_data_type);
        };
        return applyBasicBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast,
            /* drop_metric_name = */ true,
            BinaryOperatorNoGroupingTagsSource::On);
    }
    else
    {
        /// If the bool modifier isn't specified then the operator works like filter.
        chassert ((left_argument.type == ResultType::INSTANT_VECTOR) || (right_argument.type == ResultType::INSTANT_VECTOR));

        if (left_argument.type == ResultType::INSTANT_VECTOR)
            left_argument = toVectorGrid(std::move(left_argument), context);

        if (right_argument.type == ResultType::INSTANT_VECTOR)
            right_argument = toVectorGrid(std::move(right_argument), context);

        auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
        {
            ASTPtr res = (left_argument.type == ResultType::INSTANT_VECTOR) ? x->clone() : y->clone();
            return makeASTFunction("if",
                makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y)),
                std::move(res),
                make_intrusive<ASTLiteral>(Field{} /* NULL */));
        };


        return applyBasicBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast,
            /* drop_metric_name = */ false,
            BinaryOperatorNoGroupingTagsSource::Left);
    }
}

}
