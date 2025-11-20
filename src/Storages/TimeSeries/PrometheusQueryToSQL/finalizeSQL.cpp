#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>

#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/buildSelectQuery.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Prepares expressions for ORDER BY in case we sort by specific tags and then by all tags (See ResultSorting::Mode::ORDERED_BY_TAGS).
    ASTs getOrderByTagsExpressions(const Strings & sorting_tags)
    {
        ASTs list;

        /// First we sort by specified tags,
        for (const auto & sorting_tag : sorting_tags)
        {
            list.push_back(makeASTFunction(
                "timeSeriesExtractTag",
                std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group),
                std::make_shared<ASTLiteral>(sorting_tag)));
        }

        /// then by all tags.
        list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));

        return list;
    }


    /// Finalizes a SQL query returning a scalar as two columns "timestamp", "value".
    ASTPtr finalizeScalarAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::SCALAR);
        checkStartTimeEqualsToEndTime(result, context.promql_tree);

        switch (result.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT start_time, scalar_value
                SelectQueryParams params;

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTLiteral>(result.scalar_value));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT start_time, values[1]
                /// FROM <scalar_grid>
                SelectQueryParams params;

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            {
                throwStoreMethodIsNotSupported(result, context.promql_tree);
            }
        }
        UNREACHABLE();
    }


    /// Finalizes a SQL query returning a string as two columns "timestamp", "value".
    ASTPtr finalizeStringAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::STRING);
        checkStartTimeEqualsToEndTime(result, context.promql_tree);

        if (result.store_method != StoreMethod::CONST_STRING)
            throwStoreMethodIsNotSupported(result, context.promql_tree);

        /// SELECT start_time, string_value
        SelectQueryParams params;

        params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

        params.select_list.push_back(std::make_shared<ASTLiteral>(result.string_value));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

        return buildSelectQuery(std::move(params));
    }


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "timestamp", "value".
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);
        checkStartTimeEqualsToEndTime(result, context.promql_tree);

        switch (result.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)), start_time, scalar_value
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(std::make_shared<ASTLiteral>(result.scalar_value));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT []::Array(Tuple(String, String)),
                ///        start_time,
                ///        values[1]
                /// FROM <scalar_grid>
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group),
                ///        start_time,
                ///        toFloat64(values[1])
                /// FROM <vector_grid>
                /// WHERE isNotNull(values[1])
                /// [ORDER BY tags/value] [LIMIT]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(timeseriesTimeToAST(result.start_time, context.result_timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "toFloat64",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u))));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                params.where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)));

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }
                else if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_VALUE)
                {
                    params.order_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value));
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                throwStoreMethodIsNotSupported(result, context.promql_tree);
            }
        }

        UNREACHABLE();
    }


    /// Finalizes a SQL query returning a range vector as two columns "tags", "time_series".
    ASTPtr finalizeRangeVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::RANGE_VECTOR);

        switch (result.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)),
                ///        timeSeriesFromGrid(start_time, end_time, step, arrayResize([], count_of_time_steps, scalar_value))
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_timestamp_type),
                    timeseriesTimeToAST(result.end_time, context.result_timestamp_type),
                    timeseriesDurationToAST(result.step),
                    makeASTFunction(
                        "arrayResize",
                        std::make_shared<ASTLiteral>(Array{}),
                        std::make_shared<ASTLiteral>(countTimeseriesSteps(result.start_time, result.end_time, result.step)),
                        std::make_shared<ASTLiteral>(result.scalar_value))));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT []::Array(Tuple(String, String)),
                ///        timeSeriesFromGrid(start_time, end_time, step, values)
                /// FROM <scalar_grid>
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_timestamp_type),
                    timeseriesTimeToAST(result.end_time, context.result_timestamp_type),
                    timeseriesDurationToAST(result.step),
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group),
                ///        timeSeriesFromGrid(start_time, end_time, step, values)
                /// FROM <vector_grid>
                /// [ORDER BY tags] [LIMIT]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeseriesTimeToAST(result.start_time, context.result_timestamp_type),
                    timeseriesTimeToAST(result.end_time, context.result_timestamp_type),
                    timeseriesDurationToAST(result.step),
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesGroupArray(timestamp, value)
                /// FROM <raw_data>
                /// GROUP BY group
                /// [ORDER BY tags] [LIMIT]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesGroupArray",
                    makeASTFunction(
                        "CAST",
                        std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                        std::make_shared<ASTLiteral>(context.result_timestamp_type->getName())),
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value)));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                const auto & result_sorting = context.result_sorting;
                if (result_sorting.mode == ResultSorting::Mode::ORDERED_BY_TAGS)
                {
                    params.order_by = getOrderByTagsExpressions(result_sorting.sorting_tags);
                    params.order_direction = result_sorting.direction;
                }

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::CONST_STRING:
            {
                throwStoreMethodIsNotSupported(result, context.promql_tree);
            }
        }

        UNREACHABLE();
    }
}


ASTPtr finalizeSQL(SQLQueryPiece && result, ConverterContext & context)
{
    switch (result.type)
    {
        case ResultType::SCALAR:
            return finalizeScalarAsSQL(std::move(result), context);
        case ResultType::STRING:
            return finalizeStringAsSQL(std::move(result), context);
        case ResultType::INSTANT_VECTOR:
            return finalizeInstantVectorAsSQL(std::move(result), context);
        case ResultType::RANGE_VECTOR:
            return finalizeRangeVectorAsSQL(std::move(result), context);
    }
    UNREACHABLE();
}

}
