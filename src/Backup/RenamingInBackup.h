#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class ASTBackupQuery;
using DatabaseAndTableName = std::pair<String, String>;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;


/// Keeps information about renamings of databases and tables that is processed
/// while we're making a backup or while we're restoring from a backup.
class RenamingInBackup
{
public:
    /// Initializes renaming from a backup query.
    RenamingInBackup(const ASTBackupQuery & query);

    /// Changes names according to the renaming.
    DatabaseAndTableName getNewTableName(const DatabaseAndTableName & table_name) const;
    const String & getNewDatabaseName(const String & database_name) const;
    const String & getNewTemporaryTableName(const String & temporary_table_name) const;
    ASTPtr getNewCreateQuery(const ASTPtr & create_query) const;

private:
    std::map<DatabaseAndTableName, DatabaseAndTableName> old_to_new_table_names;
    std::unordered_map<String, String> old_to_new_database_names;
    std::unordered_map<String, String> old_to_new_temporary_table_names;
};

}
