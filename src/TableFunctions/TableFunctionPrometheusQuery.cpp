#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


template <bool over_range>
void TableFunctionPrometheusQuery<over_range>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    config = StoragePrometheusQuery::getConfiguration(args, context, over_range);
}


template <bool over_range>
ColumnsDescription TableFunctionPrometheusQuery<over_range>::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    PrometheusQueryToSQLConverter::TimeSeriesTableInfo time_series_table_info;
    time_series_table_info.storage_id = config.time_series_storage_id;
    time_series_table_info.timestamp_data_type = config.timestamp_type;
    time_series_table_info.value_data_type = config.scalar_type;
    PrometheusQueryToSQLConverter converter{*config.promql_query, time_series_table_info, Field{}, Field{}};
    if constexpr (over_range)
    {
        chassert(config.evaluation_range);
        converter.setEvaluationRange(PrometheusQueryToSQLConverter::EvaluationRange{*config.evaluation_range, config.timestamp_scale});
    }
    else
    {
        chassert(config.evaluation_time);
        converter.setEvaluationTime(DecimalField<DateTime64>{*config.evaluation_time, config.timestamp_scale});
    }
    return converter.getResultColumns();
}


template <bool over_range>
StoragePtr TableFunctionPrometheusQuery<over_range>::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StoragePrometheusQuery>(StorageID(getDatabaseName(), table_name), columns, config);
    res->startup();
    return res;
}


template class TableFunctionPrometheusQuery</* over_range = */ false>;
template class TableFunctionPrometheusQuery</* over_range = */ true>;

}
