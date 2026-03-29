#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLimitAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTWithByWithout.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a limit aggregation operator.
    void checkArgumentTypes(
        const PQT::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects 2 arguments, but was called with {} arguments",
                            operator_name, arguments.size());
        }

        const auto & k_arg = arguments[0];

        if (k_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR,
                            getPromQLText(k_arg, context), k_arg.type);
        }

        const auto & vector_arg = arguments[1];

        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }

    /// Converts the k parameter to an AST expression (as a UInt64) usable in SQL.
    ASTPtr getK(SQLQueryPiece && k_arg, ConverterContext & context)
    {
        switch (k_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return make_intrusive<ASTLiteral>(static_cast<UInt64>(k_arg.scalar_value));
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(k_arg.select_query), SQLSubqueryType::SCALAR});
                return makeASTFunction("toUInt64", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
            }
            default:
            {
                throwUnexpectedStoreMethod(k_arg, context);
            }
        }
    }

    /// Makes a single-parameter lambda: `param -> body`.
    ASTPtr makeLambda(const String & param, ASTPtr && body)
    {
        return makeASTFunction("lambda",
            makeASTFunction("tuple", make_intrusive<ASTIdentifier>(param)),
            std::move(body));
    }

    /// Makes a two-parameter lambda: `(param1, param2) -> body`.
    ASTPtr makeLambda(const String & param1, const String & param2, ASTPtr && body)
    {
        return makeASTFunction("lambda",
            makeASTFunction("tuple",
                make_intrusive<ASTIdentifier>(param1),
                make_intrusive<ASTIdentifier>(param2)),
            std::move(body));
    }

    /// Returns the sort key lambda for `arrayPartialSort` over an index array:
    /// - descending (topk)   : i -> -assumeNotNull(v[i])
    /// - ascending (bottomk) : i -> assumeNotNull(v[i])
    ASTPtr makeSortKeyLambda(ASTPtr && v, bool descending)
    {
        auto v_at_i = makeASTFunction("arrayElement", std::move(v), make_intrusive<ASTIdentifier>("i"));
        ASTPtr value_key = makeASTFunction("assumeNotNull", std::move(v_at_i));
        if (descending)
            value_key = makeASTFunction("negate", std::move(value_key));
        return makeLambda("i", std::move(value_key));
    }

    /// Builds the per-time-step indices expression for `topk` or `bottomk`:
    ///
    ///   arraySort(arraySlice(
    ///       arrayPartialSort(i -> ±assumeNotNull(v[i]), k,
    ///           arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(v), v)),
    ///       1, k))
    ///
    /// TODO: Consider adding new function arrayTopKIndices and arrayBottomKInidices to ClickHouse
    /// to simplify this expression.
    ASTPtr buildSortedTopKIndices(ASTPtr && k, ASTPtr && v, bool descending)
    {
        /// arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(v), v)
        auto non_null_indices = makeASTFunction("arrayFilter",
            makeLambda("i", "x", makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            makeASTFunction("arrayEnumerate", v->clone()),
            v->clone());

        /// arraySort(arraySlice(arrayPartialSort(i -> ±assumeNotNull(v[i]), k, non_null_indices), 1, k))
        /// The outer arraySort is required so that step 3 can use indexOfAssumeSorted.
        return makeASTFunction("arraySort",
            makeASTFunction("arraySlice",
                makeASTFunction("arrayPartialSort",
                    makeSortKeyLambda(std::move(v), descending),
                    k->clone(),
                    std::move(non_null_indices)),
                make_intrusive<ASTLiteral>(1u),
                k->clone()));
    }

    /// Builds the per-time-step indices expression for `limitk`:
    ///
    ///   arraySlice(arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(v), v), 1, k)
    ASTPtr buildLimitKIndices(ASTPtr && k, ASTPtr && v)
    {
        /// arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(v), v)
        auto non_null_indices = makeASTFunction("arrayFilter",
            makeLambda("i", "x", makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            makeASTFunction("arrayEnumerate", v->clone()),
            std::move(v));

        /// arraySlice(non_null_indices, 1, k)
        return makeASTFunction("arraySlice",
            std::move(non_null_indices),
            make_intrusive<ASTLiteral>(1u),
            k->clone());
    }

    using BuildIndicesASTFunc = ASTPtr (*)(ASTPtr && k, ASTPtr && v);

    struct ImplInfo
    {
        BuildIndicesASTFunc build_indices;
    };

    const ImplInfo * getImplInfo(std::string_view operator_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"topk",
             {[](ASTPtr && k, ASTPtr && v) -> ASTPtr
              { return buildSortedTopKIndices(std::move(k), std::move(v), /*descending=*/true); }}},

            {"bottomk",
             {[](ASTPtr && k, ASTPtr && v) -> ASTPtr
              { return buildSortedTopKIndices(std::move(k), std::move(v), /*descending=*/false); }}},

            {"limitk",
             {[](ASTPtr && k, ASTPtr && v) -> ASTPtr
              { return buildLimitKIndices(std::move(k), std::move(v)); }}},
        };

        auto it = impl_map.find(operator_name);
        if (it == impl_map.end())
            return nullptr;
        return &it->second;
    }
}


bool isLimitAggregationOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyLimitAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;

    const ImplInfo * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    checkArgumentTypes(operator_node, arguments, context);

    auto & k_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (k_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_arg = toVectorGrid(std::move(vector_arg), context);
    ASTPtr k = getK(std::move(k_arg), context);

    auto res = vector_arg;
    res.node = operator_node;

    /// Step 1: collect all series within each aggregation group.
    ///
    ///   SELECT groupArray(group) AS groups, arrayTranspose(groupArray(values)) AS values
    ///   FROM vector_grid
    ///   GROUP BY <by_tags_expr>
    ///
    /// `groups` is an array of original series group IDs (length N).
    /// `values` is a TxN matrix: for each time step t, an array of N series values.
    ASTPtr step1_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Group)));
        builder.select_list.back()->setAlias(ColumnNames::Groups);

        builder.select_list.push_back(makeASTFunction("arrayTranspose",
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Values))));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
        {
            bool metric_name_dropped_from_group = vector_arg.metric_name_dropped;
            ASTPtr by_tags_expr = transformGroupASTWithByWithout(
                operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, metric_name_dropped_from_group);
            builder.group_by.push_back(std::move(by_tags_expr));
        }

        step1_query = builder.getSelectQuery();
    }

    /// Step 2: for each time step, compute the selected indices.
    ///
    ///   SELECT groups, values, arrayMap(v -> <indices_expr>, values) AS indices
    ///   FROM step1
    ASTPtr step2_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Groups));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        ASTPtr indices_expr = impl_info->build_indices(std::move(k), make_intrusive<ASTIdentifier>("v"));

        builder.select_list.push_back(makeASTFunction("arrayMap",
            makeLambda("v", std::move(indices_expr)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values)));

        builder.select_list.back()->setAlias(ColumnNames::Indices);

        step2_query = builder.getSelectQuery();
    }

    /// Step 3: apply the per-time-step mask using `indices`, producing `masked_values` (TxN).
    ///
    ///   SELECT groups,
    ///       arrayMap((v, idx) -> arrayMap((x, i) -> if(indexOfAssumeSorted(idx, i) > 0, x, NULL),
    ///           v, arrayEnumerate(v)),
    ///           values, indices) AS masked_values
    ///   FROM step2
    ASTPtr step3_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Groups));

        auto inner_lambda = makeLambda("x", "i",
            makeASTFunction("if",
                makeASTFunction("greater",
                    makeASTFunction("indexOfAssumeSorted",
                        make_intrusive<ASTIdentifier>("idx"),
                        make_intrusive<ASTIdentifier>("i")),
                    make_intrusive<ASTLiteral>(0u)),
                make_intrusive<ASTIdentifier>("x"),
                make_intrusive<ASTLiteral>(Field{} /* NULL */)));

        auto mask_body = makeASTFunction("arrayMap",
            std::move(inner_lambda),
            make_intrusive<ASTIdentifier>("v"),
            makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>("v")));

        builder.select_list.push_back(makeASTFunction("arrayMap",
            makeLambda("v", "idx", std::move(mask_body)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values),
            make_intrusive<ASTIdentifier>(ColumnNames::Indices)));
        builder.select_list.back()->setAlias(ColumnNames::MaskedValues);

        step3_query = builder.getSelectQuery();
    }

    /// Step 4: transpose `masked_values` from TxN to NxT, then unzip `groups` and `masked_values`
    /// into individual series rows, discarding series that have no selected values at any time step.
    ///
    /// After step 3, `masked_values` is an array of T inner arrays, each of length N.
    /// `arrayTranspose` converts it to NxT so that `arrayZip` can pair each series group ID
    /// with its T-length values array.
    ///
    ///   SELECT (arrayJoin(arrayZip(groups, arrayTranspose(masked_values))) AS p).1 AS group,
    ///          p.2 AS values
    ///   FROM step3
    ///   WHERE arrayExists(x -> isNotNull(x), p.2)
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step3_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        auto array_join_expr = makeASTFunction("arrayJoin",
            makeASTFunction("arrayZip",
                make_intrusive<ASTIdentifier>(ColumnNames::Groups),
                makeASTFunction("arrayTranspose", make_intrusive<ASTIdentifier>(ColumnNames::MaskedValues))));
        array_join_expr->setAlias("p");

        builder.select_list.push_back(makeASTFunction("tupleElement", array_join_expr, make_intrusive<ASTLiteral>(1u)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.where = makeASTFunction("arrayExists",
            makeLambda("x", makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
