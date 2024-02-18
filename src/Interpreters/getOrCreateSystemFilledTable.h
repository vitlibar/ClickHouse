#pragma once

#include <Common/Logger.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage_fwd.h>


namespace DB
{

/// Gets or creates a table which is filled by ClickHouse itself. For example, "system.query_log".
/// If `check_create_query_if_exists` is true and the table specified in the passed `create_query` exists then
/// the function checks that the definition of the existing table matches the passed `create_query`.
/// And if it doesn't match the function will rename the existing table and then recreate it using the passed `create_query`.
/// The function returns `{storage, true}` if it's just created a storage, or `{storage, false}` if there is an existing storage which can be used.
std::pair<StoragePtr, bool /* created */> getOrCreateSystemFilledTable(
    ContextPtr context,
    ASTPtr create_query,
    bool check_create_query_if_exists = true);

}
