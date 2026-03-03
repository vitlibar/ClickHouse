#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorOr.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorAnd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applyBinaryOperatorHelpers.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece applyBinaryOperatorOr(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context)
{
    checkArgumentTypesForSetBinaryOperator(operator_node, left_argument, right_argument, context);

    /// If one of the arguments is empty then we return the other argument.
    if (left_argument.store_method == StoreMethod::EMPTY)
    {
        auto res = std::move(right_argument);
        res.node = operator_node;
        return res;
    }

    if (right_argument.store_method == StoreMethod::EMPTY)
    {
        auto res = std::move(left_argument);
        res.node = operator_node;
        return res;
    }

    left_argument = toVectorGrid(std::move(left_argument), context);
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
    String left = context.subqueries.back().name;

    right_argument = toVectorGrid(std::move(right_argument), context);
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
    String right = context.subqueries.back().name;

    /// Step 1:
    /// SELECT group,
    ///        if(notEmpty(right.values), arrayMap(x, y -> if(isNotNull(x), x, y), left.values, right.values), left.values) AS values
    /// FROM left LEFT ANY JOIN right
    /// ON left.group == right.group
    ///
    String step1;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values})),
            makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                        make_intrusive<ASTIdentifier>("x"),
                        make_intrusive<ASTIdentifier>("y"))),
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values})),
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values})));

        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.from_table = left;
        builder.join_kind = JoinKind::Left;
        builder.join_strictness = JoinStrictness::Any;
        builder.join_table = right;

        builder.join_on = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Group}),
            make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Group}));

        ASTPtr step1_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_ast), SQLSubqueryType::TABLE});
        step1 = context.subqueries.back().name;
    }

    /// Step 2:
    /// SELECT timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
    ///        countForEach(values) AS join_counts
    /// GROUP BY join_group
    /// FROM left
    ///
    String step2;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(makeExpressionForJoinGroup(
            operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), left_argument.metric_name_dropped));
        builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

        builder.select_list.push_back(makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        builder.select_list.back()->setAlias(ColumnNames::JoinCounts);

        builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

        builder.from_table = left;

        ASTPtr step2_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_ast), SQLSubqueryType::TABLE});
        step2 = context.subqueries.back().name;
    }

    /// Step 3:
    /// SELECT group,
    ///        if(notEmpty(join_counts), arrayMap(x, y -> if(x = 0, y, NULL), join_counts, values), values) AS values
    /// FROM step2 RIGHT ANY JOIN right
    /// ON join_group == timeSeriesRemoveAllTagsExcept(group, on_tags)
    String step3;
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(makeASTFunction(
            "if",
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::JoinCounts)),
            makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(0u)),
                        make_intrusive<ASTIdentifier>("y"),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                make_intrusive<ASTIdentifier>(ColumnNames::JoinCounts),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));

        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.from_table = step2;
        builder.join_kind = JoinKind::Right;
        builder.join_strictness = JoinStrictness::Any;
        builder.join_table = right;

        builder.join_on = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup),
            makeExpressionForJoinGroup(
                operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), right_argument.metric_name_dropped));

        ASTPtr step3_ast = builder.getSelectQuery();
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step3_ast), SQLSubqueryType::TABLE});
        step3 = context.subqueries.back().name;
    }

    /// Step 4:
    /// SELECT group, values FROM step1 UNION ALL SELECT group, values FROM step3
    ASTPtr step4;
    {
        SelectQueryBuilder builder;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.from_table = step1;
        builder.union_table = step3;
        step4 = builder.getSelectQuery();
    }

    SQLQueryPiece res{operator_node, ResultType::INSTANT_VECTOR, StoreMethod::VECTOR_GRID};
    res.select_query = std::move(step4);
    res.metric_name_dropped = left_argument.metric_name_dropped && right_argument.metric_name_dropped;
    return res;
}

}
