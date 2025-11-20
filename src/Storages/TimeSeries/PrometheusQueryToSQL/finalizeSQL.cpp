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
    void checkStartTimeEqualsToEndTime(const SQLQueryPiece & result, const PrometheusQueryTree & promql_tree)
    {
        if (result.start_time != result.end_time)
        {
            throw Exception(
                ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                "Query {} is of type {} and can't be executed on range ({}..{}).",
                promql_tree.getQuery(result.promql_node), Field{result.type}, Field{result.start_time}, Field{result.end_time});
        }
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
                /// SELECT start_time::timestamp_type, scalar_value::scalar_type
                SelectQueryParams params;

                params.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "CAST",
                    std::make_shared<ASTLiteral>(result.scalar_value),
                    std::make_shared<ASTLiteral>(context.scalar_type->getName())));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::GRID:
            {
                /// WITH prom1 AS (SELECT FROM <grid>)
                /// SELECT start_time::timestamp_type, values[1]::scalar_type FROM prom1
                SelectQueryParams params;

                params.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "CAST",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)),
                    std::make_shared<ASTLiteral>(context.scalar_type->getName())));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)));

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

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


    /// Finalizes a SQL query returning a string as two columns "timestamp", "value".
    ASTPtr finalizeStringAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::STRING);
        checkStartTimeEqualsToEndTime(result, context.promql_tree);

        if (result.store_method != StoreMethod::CONST_STRING)
            throwStoreMethodIsNotSupported(result, context.promql_tree);

        /// SELECT start_time::timestamp_type, string_value
        SelectQueryParams params;

        params.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_type));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

        params.select_list.push_back(std::make_shared<ASTLiteral>(result.string_value));
        params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

        return buildSelectQuery(std::move(params));
    }


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "timestamp", "value".
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);

        switch (result.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)),
                ///        start_time::timestamp_type,
                ///        scalar_value::scalar_type
                checkStartTimeEqualsToEndTime(result, context.promql_tree);
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "CAST",
                    std::make_shared<ASTLiteral>(result.scalar_value),
                    std::make_shared<ASTLiteral>(context.scalar_type->getName())));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::GRID:
            {
                /// WITH prom1 AS (SELECT FROM <grid>)
                /// SELECT timeSeriesGroupToTags(group),
                ///        start_time::timestamp_type,
                ///        values[1]::scalar_type
                /// WHERE isNotNull(values[1])
                /// [ORDER BY ...] FROM prom1
                checkStartTimeEqualsToEndTime(result, context.promql_tree);
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_type));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Timestamp);

                params.select_list.push_back(makeASTFunction(
                    "CAST",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)),
                    std::make_shared<ASTLiteral>(context.scalar_type->getName())));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Value);

                params.where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "arrayElement", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values), std::make_shared<ASTLiteral>(1u)));

                params.limit = context.limit;

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

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
                ///        arrayZip(timeSeriesRange(start_time::timestamp_type, end_time::timestamp_type, step),
                ///                 replicate(number_of_steps, [scalar_value]::Array(scalar_type)))
                SelectQueryParams params;

                params.select_list.push_back(makeASTFunction(
                    "CAST", std::make_shared<ASTLiteral>(Array{}), std::make_shared<ASTLiteral>("Array(Tuple(String, String))")));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "arrayZip",
                    makeASTFunction(
                        "timeSeriesRange",
                        timeSeriesTimestampToAST(result.start_time, context.timestamp_type),
                        timeSeriesTimestampToAST(result.end_time, context.timestamp_type),
                        timeSeriesIntervalToAST(result.step)),
                    makeASTFunction(
                        "replicate",
                        std::make_shared<ASTLiteral>(getNumberOfTimeSeriesSteps(result.start_time, result.end_time, result.step)),
                        makeASTFunction(
                            "CAST",
                            std::make_shared<ASTLiteral>(Array{result.scalar_value}),
                            std::make_shared<ASTLiteral>(fmt::format("Array({})", context.scalar_type->getName()))))));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                params.limit = context.limit;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::GRID:
            {
                /// WITH prom1 AS (SELECT FROM <grid>)
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesFromGrid(start_time::timestamp_type, end_time::timestamp_type, step, values) FROM prom1
                /// [ORDER BY tags]
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.timestamp_type),
                    timeSeriesTimestampToAST(result.end_time, context.timestamp_type),
                    timeSeriesIntervalToAST(result.step),
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Values)));

                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                /// Rows are sorted alphabetically by a sorted list of the names and values of all the tags.
                params.order_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));

                params.limit = context.limit;

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

                return buildSelectQuery(std::move(params));
            }

            case StoreMethod::RAW_DATA:
            {
                /// WITH prom1 AS (SELECT FROM <raw_data>)
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesGroupArray(timestamp, value) FROM prom1
                /// GROUP BY group ORDER BY tags
                SelectQueryParams params;

                params.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::Tags);

                params.select_list.push_back(makeASTFunction(
                    "timeSeriesGroupArray",
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                    std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Value)));
                params.select_list.back()->setAlias(TimeSeriesColumnNames::TimeSeries);

                params.group_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Group));

                /// Rows are sorted alphabetically by a sorted list of the names and values of all the tags.
                params.order_by.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Tags));

                params.limit = context.limit;

                params.with = std::move(context.subqueries);
                params.with.emplace_back(SQLSubquery{params.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                params.from_subquery = params.with.back().name;

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
