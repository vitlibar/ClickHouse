#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/StoragePrometheusQuery.h>
#include <Storages/TimeSeries/ParsedPrometheusQuery.h>
#include <Storages/TimeSeries/getPrometheusQueryOutputColumnsDesc.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SUPPORT_IS_DISABLED;
}


void TableFunctionPrometheusQuery::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;

    if ((args.size() != 2) && (args.size() != 3))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires one or two arguments: {}([database, ] time_series_table)", name, name);

    String promql_query = checkAndGetLiteralArgument<String>(evaluateConstantExpressionOrIdentifierAsLiteral(args[0], context), "promql_query");
    parsed_promql_query = std::make_shared<ParsedPrometheusQuery>(promql_query);

    if (args.size() == 2)
    {
        /// prometheusQuery( [my_db.]my_time_series_table )
        if (const auto * id = args[1]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
                time_series_storage_id = table_id->getTableId();
        }
    }

    if (time_series_storage_id.empty())
    {
        for (size_t i = 1; i != args.size(); ++i)
        {
            auto & arg = args[i];
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
        }

        if (args.size() == 2)
        {
            /// prometheusQuery( 'my_time_series_table', 'promql_query' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[1], "table_name");
        }
        else
        {
            /// timeSeriesMetrics( 'mydb', 'my_time_series_table', 'promql_query' )
            time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[1], "database_name");
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[2], "table_name");
        }
    }

    if (time_series_storage_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't get a table name from the arguments of the {} table function", name);

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);
}


ColumnsDescription TableFunctionPrometheusQuery::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    return getPrometheusQueryOutputColumnsDesc(*parsed_promql_query, time_series_storage_id, context);
}


StoragePtr TableFunctionPrometheusQuery::executeImpl(
    const ASTPtr & /* ast_function */,
    [[maybe_unused]] ContextPtr context,
    [[maybe_unused]] const String & table_name,
    ColumnsDescription /* cached_columns */,
    bool /* is_insert_query */) const
{
#if USE_ANTLR4_GRAMMARS
    auto res = std::make_shared<StoragePrometheusQuery>(
        StorageID(getDatabaseName(), table_name), parsed_promql_query, time_series_storage_id, context);
    res->startup();
    return res;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ANTLR4 support is disabled");
#endif
}


void registerTableFunctionPrometheusQuery(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPrometheusQuery>(
        {.documentation = {
            .description=R"(Executes a prometheus query on a TimeSeries table.)",
            .examples{{"prometheusQuery", "SELECT * from prometheusQuery('http_requests_total{job=\"prometheus\",group=\"canary\"}', 'mydb', 'time_series_table', );", ""}},
            .category{""}}
        });
}

}
