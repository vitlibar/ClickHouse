#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLabelReplacingFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


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

        if (function_name == "label_replace")
        {
            if (arguments.size() != 5)
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} arguments, but was called with {} arguments",
                                function_name, 5, arguments.size());
        }
        else
        {
            chassert(function_name == "label_join");
            if (arguments.size() < 3)
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} or more arguments, but was called with {} arguments",
                                function_name, 3, arguments.size());
        }

        chassert(!arguments.empty());

        const auto & first_argument = arguments[0];
        if (first_argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects the first argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR, getPromQLText(first_argument, context), first_argument.type);
        }

        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto & argument = arguments[i];
            if (argument.type != ResultType::STRING)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects argument #{} of type {}, but expression {} has type {}",
                                function_name, i + 1, ResultType::STRING, getPromQLText(argument, context), argument.type);
            }
        }
    }

    struct ImplInfo
    {
        std::string_view ch_function_name;
        size_t dest_label_argument_index = static_cast<size_t>(-1);
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"label_replace", {"timeSeriesReplaceTag", 1}},
            {"label_join",    {"timeSeriesJoinTags",   1}},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isLabelReplacingFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece
applyLabelReplacingFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    chassert(impl_info->dest_label_argument_index < arguments.size());
    auto & first_argument = arguments[0];
    const String & dest_label = arguments[impl_info->dest_label_argument_index].string_value;

    auto res = first_argument;
    res.node = function_node;

    switch (first_argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// For const scalar:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, arrayResize([], <count_of_time_steps>, <scalar_value>) AS values
            ///
            /// For single scalar:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, arrayResize([], <count_of_time_steps>, value) AS values FROM <subquery>
            ///
            /// For scalar grid:
            /// SELECT f(0, 'arg2', 'arg3', ...) AS group, values
            /// FROM <scalar_grid>
            SelectQueryBuilder builder;

            ASTs group_function_args;
            group_function_args.push_back(make_intrusive<ASTLiteral>(0u)); /// Group "0" means no tags

            for (size_t i = 1; i < arguments.size(); ++i)
                group_function_args.push_back(make_intrusive<ASTLiteral>(arguments[i].string_value));

            auto group_function = makeASTFunction(impl_info->ch_function_name);
            group_function->arguments->children = std::move(group_function_args);

            builder.select_list.push_back(std::move(group_function));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            ASTPtr values;
            if (first_argument.store_method == StoreMethod::SCALAR_GRID)
            {
                values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            }
            else
            {
                ASTPtr value = (first_argument.store_method == StoreMethod::CONST_SCALAR)
                    ? timeSeriesScalarToAST(first_argument.scalar_value, context.scalar_data_type)
                    : make_intrusive<ASTIdentifier>(ColumnNames::Value);

                values = makeASTFunction(
                    "arrayResize",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(
                        stepsInTimeSeriesRange(first_argument.start_time, first_argument.end_time, first_argument.step)),
                    value);

                values->setAlias(ColumnNames::Values);
            }

            builder.select_list.push_back(std::move(values));

            if (first_argument.select_query)
            {
                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(first_argument.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;
            }

            res.select_query = builder.getSelectQuery();
            res.store_method = StoreMethod::VECTOR_GRID;
            res.scalar_value = {};
            res.metric_name_dropped = (dest_label != kMetricName);

            return res;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// Step 1:
            /// SELECT f(group, 'arg2', 'arg3', ...) AS new_group, any(values) AS values
            /// FROM <vector_grid>
            /// GROUP BY new_group
            /// HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, new_group) = 0
            ASTPtr label_replacing_query;
            {
                SelectQueryBuilder builder;

                ASTs group_function_args;
                group_function_args.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                for (size_t i = 1; i < arguments.size(); ++i)
                    group_function_args.push_back(make_intrusive<ASTLiteral>(arguments[i].string_value));

                auto group_function = makeASTFunction(impl_info->ch_function_name);
                group_function->arguments->children = std::move(group_function_args);

                builder.select_list.push_back(std::move(group_function));
                builder.select_list.back()->setAlias(ColumnNames::NewGroup);

                builder.select_list.push_back(makeASTFunction("any", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                builder.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(first_argument.select_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::NewGroup)),
                    make_intrusive<ASTLiteral>(0u));

                label_replacing_query = builder.getSelectQuery();
            }

            /// Step 2:
            /// SELECT new_group AS group, values
            /// FROM step1
            ASTPtr column_renaming_query;
            {
                SelectQueryBuilder builder;

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
                builder.select_list.back()->setAlias(ColumnNames::Group);

                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

                context.subqueries.emplace_back(
                    SQLSubquery{context.subqueries.size(), std::move(label_replacing_query), SQLSubqueryType::TABLE});
                builder.from_table = context.subqueries.back().name;

                column_renaming_query = builder.getSelectQuery();
            }

            res.select_query = std::move(column_renaming_query);

            if (dest_label == kMetricName)
                res.metric_name_dropped = false;

            return res;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with the allowed argument types
            /// (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(first_argument, context);
        }
    }

    UNREACHABLE();
}

}
