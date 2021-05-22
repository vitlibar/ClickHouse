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

    const std::unordered_set<String> & getTableNames(const String & database_name) const;
    DatabaseAndTableName getNewTableName(const DatabaseAndTableName & table_name) const;

    const std::unordered_set<String> & getDatabaseNames() const;
    const String & getNewDatabaseName(const String & database_name) const;

    const std::unordered_set<String> & getTemporaryTableNames() const;
    const String & getNewTemporaryTableName(const String & temporary_table_name) const;

private:
    std::unordered_map<String, std::unordered_set<String>> table_names;
    std::map<DatabaseAndTableName, DatabaseAndTableName> new_table_names;
    std::unordered_set<String> database_names;
    std::unordered_map<String, String> new_database_names;
    std::unordered_set<String> temporary_table_names;
    std::unordered_map<String, String> new_temporary_table_names;
};


}
