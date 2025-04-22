#pragma once

#include <TableFunctions/ITableFunction.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ParsedPrometheusQuery;

/// Table function prometheusQuery('query', 'mydb', 'ts_table') executes a prometheus query on a TimeSeries table.
/// This table function can execute either instant or range queries.
class TableFunctionPrometheusQuery : public ITableFunction
{
public:
    static constexpr auto name = "prometheusQuery";
    String getName() const override { return name; }

private:
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "PrometheusQuery"; }

    std::shared_ptr<ParsedPrometheusQuery> parsed_promql_query;
    StorageID time_series_storage_id = StorageID::createEmpty();
};

}
