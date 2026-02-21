#include <Storages/TimeSeries/PrometheusQueryToSQL/applyDateTimeFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a date/time function.
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLQuery(argument, context), argument.type);
        }
    }

    using TransformASTFunc = ASTPtr (*)(ASTPtr t);
    using EvaluateWithConstArgumentFunc = int (*)(time_t);

    struct ImplInfo
    {
        TransformASTFunc transform_ast;
        EvaluateWithConstArgumentFunc evaluate_with_const_argument;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"day_of_week",
             {
                 /// Returned values should be from 0 to 6, where 0 means Sunday.
                 [](ASTPtr t) -> ASTPtr
                 { return makeASTFunction("toDayOfWeek", std::move(t), /* mode = */ make_intrusive<ASTLiteral>(2u)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_wday;
                 },
             }},

            {"day_of_month",
             {
                 /// Returned values should be from 1 to 31.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfMonth", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_mday;
                 },
             }},

#if 0
            /// FIXME: Implement calculating the number of days in month.
            {"days_in_month",
             {
                 /// Returned values should be from 28 to 31.
                 [](ASTPtr t) -> ASTPtr
                 {
                     /// There is no function toDaysInMonth() in ClickHouse, we could use here
                     /// dateDiff('day', toStartOfMonth(x), toLastDayOfMonth(x)) + 1
                    return makeASTFunction("toDaysInMonth", std::move(t));
                 },
                 [](time_t t) -> int
                 {
                     ...
                 },
             }},
#endif

            {"day_of_year",
             {
                 /// Returned values should be from 1 to 365 for non-leap years, and 1 to 366 in leap years.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfYear", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_yday + 1;
                 },
             }},

            {"minute",
             {
                 /// Returned values should be from 0 to 59.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMinute", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_min;
                 },
             }},

            {"hour",
             {
                 /// Returned values should be from 0 to 23.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toHour", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_hour;
                 },
             }},

            {"month",
             {
                 /// Returned values should be from 1 to 12, where 1 means January.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMonth", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_mon + 1;
                 },
             }},

            {"year",
             {
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toYear", std::move(t)); },
                 [](time_t t) -> int
                 {
                     struct tm utc_tm;
                     gmtime_r(&t, &utc_tm);
                     return utc_tm.tm_year + 1900;
                 },
             }},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isDateTimeFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyDateTimeFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);
    const auto & argument = arguments[0];

    auto res = argument;
    res.node = function_node;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        {
            time_t t = static_cast<time_t>(argument.scalar_value);
            res.scalar_value = (impl_info->evaluate_with_const_argument)(t);
            return res;
        }

        case StoreMethod::SCALAR_GRID:
        case StoreMethod::VECTOR_GRID:
        {
            /// For scalar grid:
            /// SELECT arrayMap(x -> f(x), values) AS values
            /// FROM <scalar_grid>
            ///
            /// For vector grid:
            /// SELECT group, arrayMap(x -> f(x), values) AS values
            /// FROM <vector_grid>
            SelectQueryBuilder builder;
            if (argument.store_method == StoreMethod::VECTOR_GRID)
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    timeSeriesScalarASTCast(
                        (impl_info->transform_ast)(makeASTFunction("toDateTime", make_intrusive<ASTIdentifier>("x"))),
                        context.scalar_data_type)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));

            builder.select_list.back()->setAlias(ColumnNames::Values);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            res.select_query = builder.getSelectQuery();

            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here, the store method CONST_STRING is incompatible
            /// with the allowed argument types (see checkArgumentTypes()).
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Argument {} of function '{}' has unexpected type {} (store_method: {})",
                            getPromQLQuery(argument, context), function_name, argument.type, argument.store_method);
        }
    }

    UNREACHABLE();
}

}
