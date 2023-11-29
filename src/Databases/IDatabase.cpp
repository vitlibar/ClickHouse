#include <memory>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/quoteString.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/NamePrompter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_TABLE;
    extern const int CANNOT_BACKUP_TABLE;
    extern const int CANNOT_RESTORE_TABLE;
}

StoragePtr IDatabase::getTable(const String & name, ContextPtr context) const
{
    if (auto storage = tryGetTable(name, context))
        return storage;
    TableNameHints hints(this->shared_from_this(), context);
    std::vector<String> names = hints.getHints(name);
    if (names.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name));
    else
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} does not exist. Maybe you meant {}?", backQuoteIfNeed(getDatabaseName()), backQuoteIfNeed(name), backQuoteIfNeed(names[0]));
}

std::vector<std::pair<ASTPtr, StoragePtr>> IDatabase::getTablesForBackup(const ContextPtr &, const FilterByNameFunction &, const std::optional<Strings> &, UInt32 &) const
{
    /// Cannot backup any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_BACKUP_TABLE,
                    "Database engine {} does not support backups, cannot backup tables in database {}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()));
}

Tables IDatabase::getTables(const Strings & table_names, const ContextPtr & context) const
{
    Tables res;
    for (const auto & table_name : table_names)
        res.emplace(table_name, getTable(table_name));
    return res;
}

Tables IDatabase::getTablesByFilter(const FilterByNameFunction & filter_by_table_name, const ContextPtr & context) const
{
    Tables res;
    for (auto it = getTablesIterator(context); it->isValid(); it->next())
    {
        const auto & table_name = it->name();
        const auto & storage = it->table();
        if (!storage || !filter_by_name(table_name))
            continue; /// Probably the table has been just dropped.

        res.emplace(table_name, storage);
    }
    return res;
}

std::map<String, String> IDatabase::getConsistentMetadataSnapshot(const ContextPtr &, const Strings &, size_t, UInt32 &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Database engine {} does not support consistent metadata snapshots", getEngineName());
}

std::map<String, String>
IDatabase::getConsistentMetadataSnapshotByFilter(const ContextPtr &, const FilterByNameFunction &, size_t, UInt32 &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Database engine {} does not support consistent metadata snapshots", getEngineName());
}

void IDatabase::createTableRestoredFromBackup(const ASTPtr & create_table_query, ContextMutablePtr, std::shared_ptr<IRestoreCoordination>, UInt64)
{
    /// Cannot restore any table because IDatabase doesn't own any tables.
    throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE,
                    "Database engine {} does not support restoring tables, cannot restore table {}.{}",
                    getEngineName(), backQuoteIfNeed(getDatabaseName()),
                    backQuoteIfNeed(create_table_query->as<const ASTCreateQuery &>().getTable()));
}

}
