#include <TableFunctions/TableFunctionNoMergeFilter.h>

#include <Common/Exception.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    [[noreturn]] void throwUnreachable(const char * method)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table function '{}' must be unwrapped by QueryTreeBuilder::tryBuildNoMergeFilterTableExpression "
            "before reaching {}. Reaching this path indicates a bug.",
            TableFunctionNoMergeFilter::name, method);
    }
}

void TableFunctionNoMergeFilter::parseArguments(const ASTPtr & /*ast_function*/, ContextPtr /*context*/)
{
    throwUnreachable("parseArguments");
}

ColumnsDescription TableFunctionNoMergeFilter::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    throwUnreachable("getActualTableStructure");
}

StoragePtr TableFunctionNoMergeFilter::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr /*context*/, const std::string & /*table_name*/,
    ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    throwUnreachable("executeImpl");
}

void registerTableFunctionNoMergeFilter(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionNoMergeFilter>(
        FunctionDocumentation{
            .description =
                "Pragma-style wrapper used in `FROM`. Prevents the query-plan optimizer from fusing outer filters "
                "with the expressions/filters produced by its single argument (a subquery, a CTE name, a table name, "
                "or a nested table function). Unlike `view(...)`, the argument is resolved in the surrounding scope, "
                "so outer CTEs and aliases are visible inside. Has no runtime effect of its own — it only inserts an "
                "optimization barrier between the outer and inner parts of the query plan.",
        },
        {.allow_readonly = true});
}

}
