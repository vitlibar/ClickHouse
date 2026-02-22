#include <Storages/TimeSeries/PrometheusQueryToSQL/finalizeSQL.h>

#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    [[noreturn]] void throwCannotFinalize(const SQLQueryPiece & result, const ConverterContext & context)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot finalize expression {} with type: {}, store method: {}, start_time: {}, end_time: {}, step: {}",
                        getPromQLText(result, context),
                        result.type,
                        result.store_method,
                        toString(result.start_time, context.timestamp_scale),
                        toString(result.end_time, context.timestamp_scale),
                        toString(result.step, context.timestamp_scale));
    }

    /// Finalizes a SQL query returning a scalar as two columns "time", "value".
    ASTPtr finalizeScalarAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::SCALAR);

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                throwCannotFinalize(result, context);
            }

            case StoreMethod::CONST_SCALAR:
            {
                if (result.start_time != result.end_time)
                    throwCannotFinalize(result, context);

                /// SELECT <start_time> AS timestamp, <scalar_value> AS value
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                return builder.getSelectQuery();
            }

            case StoreMethod::SCALAR_GRID:
            {
                if (result.start_time != result.end_time)
                    throwCannotFinalize(result, context);

                /// SELECT <start_time> AS timestamp, values[1] AS value
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    context.scalar_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::VECTOR_GRID:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because these store methods are incompatible with ResultType::SCALAR.
                throwUnexpectedStoreMethod(result, context);
            }
        }
        UNREACHABLE();
    }


    /// Finalizes a SQL query returning a string as two columns "time", "value".
    ASTPtr finalizeStringAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::STRING);

        if (result.store_method != StoreMethod::CONST_STRING)
        {
            /// Can't get in here because other store methods are incompatible with ResultType::STRING.
            throwUnexpectedStoreMethod(result, context);
        }

        if (result.start_time != result.end_time)
            throwCannotFinalize(result, context);

        /// SELECT <start_time> AS timestamp, 'string_value' AS value
        /// [LIMIT ...]
        SelectQueryBuilder builder;

        builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Timestamp);

        builder.select_list.push_back(make_intrusive<ASTLiteral>(result.string_value));
        builder.select_list.back()->setAlias(ColumnNames::Value);

        return builder.getSelectQuery();
    }


    /// Finalizes a SQL query returning an instant vector as three columns "tags", "time", "value".
    ASTPtr finalizeInstantVectorAsSQL(SQLQueryPiece && result, ConverterContext & context)
    {
        chassert(result.type == ResultType::INSTANT_VECTOR);

        switch (result.store_method)
        {
            case StoreMethod::EMPTY:
            {
                /// SELECT arrayJoin([]::Array(Array(Tuple(String, String))) AS tags,
                ///        defaultValueOfTypeName(timestamp_data_type) AS timestamp,
                ///        defaultValueOfTypeName(scalar_data_type) AS value
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Array(Tuple(String, String)))"))));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(
                    makeASTFunction("defaultValueOfTypeName", make_intrusive<ASTLiteral>(context.timestamp_data_type->getName())));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(
                    makeASTFunction("defaultValueOfTypeName", make_intrusive<ASTLiteral>(context.scalar_data_type->getName())));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                if (result.start_time != result.end_time)
                    throwCannotFinalize(result, context);

                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        <start_time> AS timestamp,
                ///        <scalar_value> AS value
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                return builder.getSelectQuery();
            }

            case StoreMethod::SCALAR_GRID:
            {
                if (result.start_time != result.end_time)
                    throwCannotFinalize(result, context);

                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        <start_time> AS timestamp,
                ///        values[1] AS value
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    context.scalar_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                return builder.getSelectQuery();
            }

            case StoreMethod::VECTOR_GRID:
            {
                if (result.start_time != result.end_time)
                    throwCannotFinalize(result, context);

                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        <start_time> AS timestamp,
                ///        values[1] AS value
                /// FROM <vector_grid>
                /// WHERE isNotNull(values[1])
                /// [ORDER BY tags/value]
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group)));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Timestamp);

                builder.select_list.push_back(timeSeriesScalarASTCast(
                    makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)),
                    context.scalar_data_type));
                builder.select_list.back()->setAlias(ColumnNames::Value);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                builder.where = makeASTFunction(
                    "isNotNull",
                    makeASTFunction(
                        "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)));

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_STRING:
            case StoreMethod::RAW_DATA:
            {
                /// Can't get in here because these store methods are incompatible with ResultType::INSTANT_VECTOR.
                throwUnexpectedStoreMethod(result, context);
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
            case StoreMethod::EMPTY:
            {
                /// SELECT arrayJoin([]::Array(Array(Tuple(String, String)))) AS tags,
                ///        defaultValueOfTypeName(Array(Tuple(timestamp_data_type, scalar_data_type))) AS time_series
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "arrayJoin",
                    makeASTFunction(
                        "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Array(Tuple(String, String)))"))));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(makeASTFunction(
                    "defaultValueOfTypeName",
                    make_intrusive<ASTLiteral>(
                        fmt::format("Array(Tuple({}, {}))", context.timestamp_data_type->getName(), context.scalar_data_type->getName()))));
                builder.select_list.back()->setAlias(ColumnNames::TimeSeries);

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_SCALAR:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           arrayResize([], <count_of_time_steps>, <scalar_value>)) AS time_series
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(result.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(result.step, context.timestamp_data_type),
                    makeASTFunction(
                        "arrayResize",
                        make_intrusive<ASTLiteral>(Array{}),
                        make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(result.start_time, result.end_time, result.step)),
                        timeSeriesScalarToAST(result.scalar_value, context.scalar_data_type))));

                builder.select_list.back()->setAlias(ColumnNames::TimeSeries);

                return builder.getSelectQuery();
            }

            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT []::Array(Tuple(String, String)) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           CAST(values, Array(scalar_data_type))) AS time_series
                /// FROM <scalar_grid>
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(makeASTFunction(
                    "CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(Tuple(String, String))")));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(result.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(result.step, context.timestamp_data_type),
                    makeASTFunction(
                        "CAST",
                        make_intrusive<ASTIdentifier>(ColumnNames::Values),
                        make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName())))));

                builder.select_list.back()->setAlias(ColumnNames::TimeSeries);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                return builder.getSelectQuery();
            }

            case StoreMethod::VECTOR_GRID:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesFromGrid(<start_time>, <end_time>, <step>,
                ///                           CAST(values, Array(Nullable(scalar_data_type)))) AS time_series
                /// FROM <vector_grid>
                /// [ORDER BY tags]
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group)));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesFromGrid",
                    timeSeriesTimestampToAST(result.start_time, context.timestamp_data_type),
                    timeSeriesTimestampToAST(result.end_time, context.timestamp_data_type),
                    timeSeriesDurationToAST(result.step, context.timestamp_data_type),
                    makeASTFunction(
                        "CAST",
                        make_intrusive<ASTIdentifier>(ColumnNames::Values),
                        make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_data_type->getName())))));

                builder.select_list.back()->setAlias(ColumnNames::TimeSeries);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                builder.where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::TimeSeries));

                /// Data from range queries comes sorted alphabetically by tags.
                builder.order_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));
                builder.order_direction = 1;

                return builder.getSelectQuery();
            }

            case StoreMethod::RAW_DATA:
            {
                /// SELECT timeSeriesGroupToTags(group) AS tags,
                ///        timeSeriesGroupArray(timestamp, value) AS time_series
                /// FROM <raw_data>
                /// GROUP BY group
                /// [ORDER BY tags]
                /// [LIMIT ...]
                SelectQueryBuilder builder;

                builder.select_list.push_back(
                    makeASTFunction("timeSeriesGroupToTags", make_intrusive<ASTIdentifier>(ColumnNames::Group)));
                builder.select_list.back()->setAlias(ColumnNames::Tags);

                builder.select_list.push_back(makeASTFunction(
                    "timeSeriesGroupArray",
                    timeSeriesTimestampASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Timestamp), context.timestamp_data_type),
                    timeSeriesScalarASTCast(make_intrusive<ASTIdentifier>(ColumnNames::Value), context.scalar_data_type)));

                builder.select_list.back()->setAlias(ColumnNames::TimeSeries);

                builder.with = std::move(context.subqueries);
                builder.with.emplace_back(SQLSubquery{builder.with.size(), std::move(result.select_query), SQLSubqueryType::TABLE});
                builder.from_table = builder.with.back().name;

                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                /// Data from range queries comes sorted alphabetically by tags.
                builder.order_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));
                builder.order_direction = 1;

                return builder.getSelectQuery();
            }

            case StoreMethod::CONST_STRING:
            {
                /// Can't get in here because this store method are incompatible with ResultType::RANGE_VECTOR.
                throwUnexpectedStoreMethod(result, context);
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
