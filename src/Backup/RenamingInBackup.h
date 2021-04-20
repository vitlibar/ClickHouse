#pragma once

#include <Core/Types.h>
#include <map>
#include <unordered_map>


namespace DB
{
using DatabaseAndTableName = std::pair<String, String>;

/// Keeps information about renamings of databases and tables that is processed
/// while we're making a backup or while we're restoring from a backup.
class RenamingInBackup
{
public:
    RenamingInBackup();
    ~RenamingInBackup();
    RenamingInBackup(RenamingInBackup && src);
    RenamingInBackup & operator =(RenamingInBackup && src);

    void add(const String & old_database_name, const String & new_database_name);
    void add(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name);

    String getNewName(const String & old_database_name) const;
    DatabaseAndTableName getNewName(const DatabaseAndTableName & old_table_name) const;

private:
    std::unordered_map<String, String> databases;
    std::map<DatabaseAndTableName, DatabaseAndTableName> tables;
};

using RenamingInBackupPtr = std::shared_ptr<const RenamingInBackup>;

}
