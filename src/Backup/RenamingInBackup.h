#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class ASTBackupQuery;
using DatabaseAndTableName = std::pair<String, String>;


/// Keeps information about renamings of databases and tables that is processed
/// while we're making a backup or while we're restoring from a backup.
class RenamingInBackup
{
public:
    RenamingInBackup(const ASTBackupQuery & query);

    DatabaseAndTableName getNewTableName(const DatabaseAndTableName & table_name) const;
    const String & getNewDatabaseName(const String & database_name) const;
    const String & getNewTemporaryTableName(const String & temporary_table_name) const;

    /// Applies
    ASTPtr applyToCreateQuery(const ASTPtr & create_query) const;

private:
    std::map<DatabaseAndTableName, DatabaseAndTableName> table_names;
    std::unordered_map<String, String> database_names;
    std::unordered_map<String, String> temporary_table_names;
};


}
