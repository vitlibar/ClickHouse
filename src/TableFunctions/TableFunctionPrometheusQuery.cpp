#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Parsers/ASTFunction.h>
#include <Storages/StoragePrometheusQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


template <bool is_query_range>
void TableFunctionPrometheusQuery<is_query_range>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    configuration = StoragePrometheusQuery::getConfiguration(args, context, is_query_range);
}


template <bool is_query_range>
ColumnsDescription
TableFunctionPrometheusQuery<is_query_range>::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    PrometheusQueryResultType result_type;
    if (!configuration.evaluation_time.isNull())
        result_type = configuration.promql_query.getResultType();
    else if (!configuration.evaluation_range.isNull())
        result_type = PrometheusQueryResultType::RANGE_VECTOR;
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Either evaluation time or evaluation range should be set");

    return PrometheusQueryToSQL::getResultColumns(result_type, configuration.timestamp_type, configuration.scalar_type);
}


template <bool is_query_range>
StoragePtr TableFunctionPrometheusQuery<is_query_range>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /* cached_columns */,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StoragePrometheusQuery>(StorageID(getDatabaseName(), table_name), columns, configuration);
    res->startup();
    return res;
}


template class TableFunctionPrometheusQuery</* is_query_range = */ false>;
template class TableFunctionPrometheusQuery</* is_query_range = */ true>;

}
