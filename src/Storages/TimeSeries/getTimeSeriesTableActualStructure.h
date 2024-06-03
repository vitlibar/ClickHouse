#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
class ASTCreateQuery;
class ColumnsDescription;

/// Adds missing columns and also reorders columns from the create query.
/// This function is called by InterpreterCreateQuery to build a corrected version of the table's columns.
ColumnsDescription getTimeSeriesTableActualStructure(
    const ContextPtr & local_context, const ASTCreateQuery & create_query, const ColumnsDescription & columns_from_create_query);

}
