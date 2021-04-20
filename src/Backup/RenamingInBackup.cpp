#include <Backup/RenamingInBackup.h>


namespace DB
{

RenamingInBackup::RenamingInBackup() = default;
RenamingInBackup::~RenamingInBackup() = default;
RenamingInBackup::RenamingInBackup(RenamingInBackup && src) = default;
RenamingInBackup & RenamingInBackup::operator =(RenamingInBackup && src) = default;

void RenamingInBackup::add(const String & old_database_name, const String & new_database_name)
{
    databases[old_database_name] = new_database_name;
}

void RenamingInBackup::add(const DatabaseAndTableName & old_table_name, const DatabaseAndTableName & new_table_name)
{
    tables[old_table_name] = new_table_name;
}

String RenamingInBackup::getNewName(const String & old_database_name) const
{
    auto it = databases.find(old_database_name);
    if (it != databases.end())
        return it->second;
    return old_database_name;
}

DatabaseAndTableName RenamingInBackup::getNewName(const DatabaseAndTableName & old_table_name) const
{
    auto it = tables.find(old_table_name);
    if (it != tables.end())
        return it->second;
    return {getNewName(old_table_name.first), old_table_name.second};
}

}
