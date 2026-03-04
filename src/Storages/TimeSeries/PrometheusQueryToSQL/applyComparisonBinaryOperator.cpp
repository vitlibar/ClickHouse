#if 0
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyComparisonBinaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SimpleFunctionArgumentHelper.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorHelpers.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>


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
                            "Binary operator '{}' on scalars requires bool modifier",
                            operator_name);
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

    /// Applies a math-like operator if at least one operand is scalar.
    SQLQueryPiece applyComparisonOperatorToScalar(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::string_view ch_function_name)
    {
        SimpleFunctionArgumentHelper left{0, std::move(left_argument), context};
        SimpleFunctionArgumentHelper right{1, std::move(right_argument), context};
        auto result_store_method = getResultStoreMethod(left, right);

        SelectQueryBuilder builder;

        if (result_store_method == StoreMethod::VECTOR_GRID)
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        auto transform_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr { return makeASTFunction(ch_function_name, x, y); };

        builder.select_list.push_back(makeExpressionToEvaluateSimpleFunction(transform_ast, left, right));

        builder.select_list.back()->setAlias((result_store_method == StoreMethod::SINGLE_SCALAR) ? ColumnNames::Value : ColumnNames::Values);

        builder.from_table = getTableToSelectFrom(left, right);

        SQLQueryPiece res{operator_node, operator_node->result_type, result_store_method};

        res.select_query = builder.getSelectQuery();
        res.start_time = left.start_time;
        res.end_time = left.end_time;
        res.step = left.step;

        return dropMetricName(std::move(res), context);
    }


    /// Applied a math-like operator if both operands are instant vectors.
    SQLQueryPiece applyComparisonOperatorToVectors(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::string_view ch_function_name)
    {
        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        bool metric_name_dropped_from_join_group;
        auto join_group = makeExpressionForJoinGroup(
            operator_node,
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            left_argument.metric_name_dropped,
            &metric_name_dropped_from_join_group);

        bool metric_name_dropped_from_result;
        auto result_group = makeExpressionForResultGroup(
            operator_node,
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Group}),
            make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup),
            left_argument.metric_name_dropped,
            right_argument.metric_name_dropped,
            metric_name_dropped_from_join_group,
            &metric_name_dropped_from_result);

        /// Step 1:
        /// SELECT timeSeriesCopyTags(join_group, left.group, tags_to_copy) AS new_group,
        ///        arrayMap(x, y -> x + y, left.values, right.values) AS values
        /// FROM left INNER ALL JOIN right
        /// ON (timeSeriesRemoveAllTagsExcept(left.group, on_tags) AS join_group) == timeSeriesRemoveAllTagsExcept(right.group, on_tags)
        /// GROUP BY new_group
        /// HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, new_group) = 0
        String step1;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(result_group);
            builder.select_list.back()->setAlias(ColumnNames::NewGroup);

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(ch_function_name, make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values})));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            builder.from_table = left;

            builder.join_kind = JoinKind::Inner;
            builder.join_strictness = JoinStrictness::All;
            builder.join_table = right;

            join_group->setAlias(ColumnNames::JoinGroup);
            builder.join_on = makeASTFunction(
                "equals",
                std::move(join_group),
                makeExpressionForJoinGroup(
                    operator_node, make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Group}), right_argument.metric_name_dropped));

            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

            builder.having = makeASTFunction(
                "equals",
                makeASTFunction(
                    "timeSeriesThrowDuplicateSeriesIf",
                    makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                    make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
                make_intrusive<ASTLiteral>(0u));

            ASTPtr step1_ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
            step1 = context.subqueries.back().name;
        }

        /// Step 2:
        /// SELECT new_group AS group, values
        /// FROM step1
        ASTPtr step2;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            builder.from_table = step1;

            step2 = builder.getSelectQuery();
        }

        SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};

        res.select_query = step2;
        res.metric_name_dropped = metric_name_dropped_from_result;
        res.start_time = left_argument.start_time;
        res.end_time = left_argument.end_time;
        res.step = left_argument.step;

        return res;
    }
}


bool isComparisonBinaryOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyComparisonBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;
    const auto * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    if (operator_node->bool_modifier)
    {
        return applyMathLikeBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context, impl_info->ch_function_name);
    }

    /// If one of the arguments is empty then the result is also empty.
    if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
    {
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
    }

    if ((left_argument.type == ResultType::SCALAR) || (right_argument.type == ResultType::SCALAR))
    {
        /// At least one operand is scalar.
        return applyComparisonOperatorToScalar(operator_node, std::move(left_argument), std::move(right_argument), context, impl_info->ch_function_name);
    }

    /// Both operands are instant vectors.
    chassert((left_argument.type == ResultType::INSTANT_VECTOR) && (right_argument.type == ResultType::INSTANT_VECTOR));
    return applyComparisonOperatorToVectors(operator_node, std::move(left_argument), std::move(right_argument), context, impl_info->ch_function_name);
}

}
#endif
