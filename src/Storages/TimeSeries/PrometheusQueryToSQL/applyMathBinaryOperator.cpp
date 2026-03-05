#include <Storages/TimeSeries/PrometheusQueryToSQL/applyMathBinaryOperator.h>

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

        if (operator_node->bool_modifier)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' doesn't allow bool modifier",
                            operator_name);
        }

        if ((left_argument.type != ResultType::INSTANT_VECTOR) || (right_argument.type != ResultType::INSTANT_VECTOR))
        {
            if (operator_node->group_left || operator_node->group_right)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Binary operator '{}' with the group modifier expectes two arguments of type {}, got {} and {}",
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

    /// Applies a math-like binary operator to operands if at least one of them is scalar.
    /// Other operand can be either scalar or instant vector.
    SQLQueryPiece applyMathLikeOperatorToScalarsOrVectorAndScalar(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::function<ASTPtr(ASTPtr, ASTPtr)> apply_operator_to_ast)
    {
        auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
        {
            chassert(args.size() == 2);
            return apply_operator_to_ast(args[0], args[1]);
        };

        std::vector<SQLQueryPiece> arguments;
        arguments.push_back(std::move(left_argument));
        arguments.push_back(std::move(right_argument));

        auto res = applySimpleFunctionHelper(function_node, context, apply_function_to_ast, std::move(arguments));
        return dropMetricName(std::move(res), context);
    }

    /// Applied a math-like operator if both operands are instant vectors.
    SQLQueryPiece applyMathLikeOperatorToVectors(
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast)
    {
        left_argument = toVectorGrid(std::move(left_argument), context);
        right_argument = toVectorGrid(std::move(right_argument), context);

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        String left = context.subqueries.back().name;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        String right = context.subqueries.back().name;

        const auto & on_labels = operator_node->labels;
        bool group_left = operator_node->group_left;
        bool group_right = operator_node->group_right;
        const auto & extra_labels = operator_node->extra_labels;

        /// Step 1:
        /// SELECT timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        [group, ]
        ///        values
        /// FROM left
        /// [GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, join_group) = 0]
        ///
        String step1;
        bool metric_name_dropped_from_join_group = false;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(makeExpressionForJoinGroup(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                left_argument.metric_name_dropped,
                &metric_name_dropped_from_join_group));

            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            /// We add column `group` only if we need it at step 3.
            if (group_left || (group_right && !extra_labels.empty()))
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            builder.from_table = left;

            /// If `group_left` is not specified it's either one-to-one or one-to-many matches.
            bool need_check_one_on_left = !group_left;

            if (need_check_one_on_left && (!on_labels.empty() || !left_argument.metric_name_dropped))
            {
                /// We throw an exception if there are many matches on the left but group_left isn't specified.
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup)),
                    make_intrusive<ASTLiteral>(0u));
            }

            ASTPtr step1_ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
            step1 = context.subqueries.back().name;
        }

        /// Step 2:
        /// SELECT timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        [group, ] (if group_right or group_left with extra labels)
        ///        values
        /// FROM right
        /// [GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, join_group) = 0] (if not group_right)
        ///
        String step2;
        {
            SelectQueryBuilder builder;

            bool metric_name_dropped_from_join_group_2 = false;

            builder.select_list.push_back(makeExpressionForJoinGroup(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                right_argument.metric_name_dropped,
                &metric_name_dropped_from_join_group));

            metric_name_dropped_from_join_group |= metric_name_dropped_from_join_group_2;

            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            /// We add column `group` only if we need it at step 3.
            if (group_right || (group_left && !extra_labels.empty()))
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

            builder.from_table = left;

            /// If `group_right` is not specified it's either one-to-one or many-to-one matches.
            bool need_check_one_on_right = !group_right;

            if (need_check_one_on_right && (!on_labels.empty() || !right_argument.metric_name_dropped))
            {
                /// We throw an exception if there are many matches on the right but group_right isn't specified.
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup)),
                    make_intrusive<ASTLiteral>(0u));
            }

            ASTPtr step2_ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_ast), SQLSubqueryType::TABLE});
            step2 = context.subqueries.back().name;
        }

        /// Step 3:
        /// without group_left or group_right:
        /// SELECT timeSeriesRemoveTag(join_group, '__name__') AS group,
        ///        arrayMap(x, y -> f(x, y), step1.values, step2.values) AS values
        /// FROM step1 INNER ANY JOIN step2
        /// ON step1.join_group = step2.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0]
        ///
        /// with group_left/group_right:
        /// SELECT timeSeriesCopyTag(timeSeriesRemoveTag(side_many.group, '__name__'), side_one, extra_labels) AS group,
        ///        arrayMap(x, y -> f(x, y), step1.values, step2.values) AS values
        /// FROM step1 INNER ANY JOIN step2
        /// ON step1.join_group = step2.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0]
        ///
        ASTPtr step3;
        bool metric_name_dropped_from_result = false;
        {
            SelectQueryBuilder builder;
            bool need_check_no_duplicate_group = false;
            ASTPtr new_group;
            JoinKind join_kind = JoinKind::Inner;
            JoinStrictness join_strictness = JoinStrictness::Unspecified;

            if (!group_left && !group_right)
            {
                /// Neither group_left nor group_right is specified.
                new_group = make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup);
                if (!metric_name_dropped_from_join_group)
                {
                    /// Usually the metric name is already dropped from the join group, however
                    /// it can be not dropped if it's specified explicitly in on(), for example a + on(__name__) b
                    new_group = makeASTFunction("timeSeriesRemoveTag", new_group, make_intrusive<ASTLiteral>(kMetricName));
                    need_check_no_duplicate_group = true;
                }
                metric_name_dropped_from_result = true;

                join_kind = JoinKind::Inner;
                join_strictness = JoinStrictness::Any;
            }
            else
            {
                chassert(group_left != group_right);

                /// Either group_left or group_right is specified.
                /// So there are two sides: "one" and "many".
                String side_many;
                String side_one;
                bool metric_name_dropped_from_side_many = false;
                bool metric_name_dropped_from_side_one = false;

                if (group_left)
                {
                    side_many = step1;
                    side_one = step2;
                    metric_name_dropped_from_side_many = left_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = right_argument.metric_name_dropped;
                    join_kind = JoinKind::Left;
                }
                else
                {
                    side_many = step2;
                    side_one = step1;
                    metric_name_dropped_from_side_many = right_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = left_argument.metric_name_dropped;
                    join_kind = JoinKind::Right;
                }

                join_strictness = JoinStrictness::Semi;

                /// Drop the metric name from side "many" and add extra labels from side "one".
                new_group = make_intrusive<ASTIdentifier>(Strings{side_many, ColumnNames::Group});
                if (!metric_name_dropped_from_side_many)
                {
                    new_group = makeASTFunction("timeSeriesRemoveTag", new_group, make_intrusive<ASTLiteral>(kMetricName));
                    need_check_no_duplicate_group = true;
                }
                metric_name_dropped_from_result = true;

                if (!extra_labels.empty())
                {
                    new_group = makeASTFunction(
                        "timeSeriesCopyTags",
                        new_group,
                        make_intrusive<ASTIdentifier>(Strings{side_one, ColumnNames::Group}),
                        make_intrusive<ASTLiteral>(Array{extra_labels.begin(), extra_labels.end()}));

                    need_check_no_duplicate_group = true;

                    if ((std::find(extra_labels.begin(), extra_labels.end(), kMetricName) != extra_labels.end())
                        && !metric_name_dropped_from_side_one)
                    {
                        metric_name_dropped_from_result = false;
                    }
                }
            }

            builder.select_list.push_back(std::move(new_group));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    apply_function_to_ast(make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values})));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            builder.from_table = step1;

            builder.join_kind = join_kind;
            builder.join_strictness = join_strictness;
            builder.join_table = step2;

            if (need_check_no_duplicate_group)
            {
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                    make_intrusive<ASTLiteral>(0u));
            }

            step3 = builder.getSelectQuery();
        }

        SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};

        res.select_query = std::move(step3);
        res.start_time = left_argument.start_time;
        res.end_time = left_argument.end_time;
        res.step = left_argument.step;
        res.metric_name_dropped = metric_name_dropped_from_result;

        return res;
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

    auto apply_function_to_ast = [&](ASTPtr x, ASTPtr y) -> ASTPtr
    {
        return makeASTFunction(impl_info->ch_function_name, std::move(x), std::move(y));
    };

    return applyMathLikeBinaryOperator(operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast);
}


SQLQueryPiece applyMathLikeBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast)
{
    checkArgumentTypes(operator_node, left_argument, right_argument, context);

    /// If one of the arguments is empty then the result is also empty.
    if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
    {
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
    }

    if ((left_argument.type == ResultType::SCALAR) || (right_argument.type == ResultType::SCALAR))
    {
        /// At least one operand is scalar.
        return applyMathLikeOperatorToScalarsOrVectorAndScalar(operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast);
    }

    /// Both operands are instant vectors.
    chassert((left_argument.type == ResultType::INSTANT_VECTOR) && (right_argument.type == ResultType::INSTANT_VECTOR));
    return applyMathLikeOperatorToVectors(operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast);
}

}
