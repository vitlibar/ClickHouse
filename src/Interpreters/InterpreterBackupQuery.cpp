#include <Interpreters/InterpreterBackupQuery.h>


namespace DB
{
BlockIO InterpreterBackupQuery::execute()
{
    UNUSED(context);
    UNUSED(query_ptr);
    return {};
}

}

#if 0
BackupEntries DatabaseWithOwnTablesBase::backupTable(const String & table_name, const Context & context) const
{
    auto storage = tryGetTable(table_name, context);
    if (!storage)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {}.{} doesn't exist",
                        backQuote(getDatabaseName()), backQuote(table_name));

    auto get_attach_query = [&]
    {
        auto ast = typeid_cast<std::shared_ptr<ASTCreateQuery>>(getCreateTableQuery(table_name, context));
        ast->attach = true;
        ast->uuid = UUIDHelpers::Nil;
        return serializeAST(*ast);
    };

    String attach_query = get_attach_query();
    BackupEntries entries;

    while (true)
    {
        auto entries = storage->backup(context);
        String new_attach_query = get_attach_query();
        if (new_attach_query == attach_query)
            break;
        attach_query = std::move(new_attach_query);
    }

    entries.push_back(std::make_unique<BackupEntryFromMemory>(
                "metadata/" + escapeForFileName(getDatabaseName()) + "/" + escapeForFileName(table_name) + ".sql",
                attach_query));

    return entries;
}

BackupEntries DatabaseOnDisk::backupTable(const String & table_name) const
{
    auto storage = tryGetTable(table_name, global_context);
    if (!storage)
        return {};
    BackupEntries entries = storage->backup();
    String metadata_entry_name = "metadata/" + getDatabaseName() + escapeForFileName(table_name) + ".sql";
    entries.push_back(std::make_unique<BackupEntryFromFile>(metadata_entry_name, getObjectMetadataPath(table_name)));
    return entries;
}

void DatabaseOnDisk::restoreTable(const IBackup & backup, const String & table_name)
{
    String metadata_entry_name = "metadata/" + getDatabaseName() + escapeForFileName(table_name) + ".sql";
    if (!backup.exists(metadata_entry_name))
        return;
    auto entry = backup.read(metadata_entry_name);
    auto
    copyData(entry.getReadBuffer(),

    auto storage = tryGetTable(table_name, global_context);
    if (!storage)
        return {};
    BackupEntries entries = storage->backup();
    entries.push_back(std::make_unique<BackupEntryFromFile>(
        "metadata/" + getDatabaseName() + escapeForFileName(object_name) + ".sql", getObjectMetadataPath(table_name)));
    return entries;
}
#endif
