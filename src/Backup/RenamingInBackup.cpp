#include <Backup/RenamingInBackup.h>
#include <Parsers/ASTBackupQuery.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}


RenamingInBackup::RenamingInBackup(const ASTBackupQuery & query)
{
    for (const auto & entry : query.entries)
    {
        switch (entry.type)
        {
            case ASTBackupQuery::TABLE: [[fallthrough]];
            case ASTBackupQuery::DICTIONARY:
            {
                const auto & name = entry.name;
                const auto & new_name = entry.new_name;
                if (name.first.empty() || name.second.empty() || new_name.first.empty() || new_name.second.empty())
                    throw Exception("Invalid backup query", ErrorCodes::LOGICAL_ERROR);
                if (new_table_names.count(name))
                    throw Exception(
                        "Same table " + backQuote(name.first) + "." + backQuote(name.second) + " was specified twice",
                        ErrorCodes::BAD_ARGUMENTS);
                new_table_names[name] = new_name;
                table_names[name.first].insert(name.second);
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                const auto & name = entry.name.first;
                const auto & new_name = entry.new_name.first;
                if (name.empty() || new_name.empty() || !entry.name.second.empty() || !entry.new_name.second.empty())
                    throw Exception("Invalid backup query", ErrorCodes::LOGICAL_ERROR);
                if (new_database_names.count(name))
                    throw Exception(
                        "Same database " + backQuote(name) + " was specified twice",
                        ErrorCodes::BAD_ARGUMENTS);
                new_database_names[name] = new_name;
                database_names.insert(name);
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                const auto & name = entry.name.second;
                const auto & new_name = entry.new_name.second;
                if (name.empty() || new_name.empty() || !entry.name.first.empty() || !entry.new_name.first.empty())
                    throw Exception("Invalid backup query", ErrorCodes::LOGICAL_ERROR);
                if (new_temporary_table_names.count(name))
                    throw Exception(
                        "Same temporary table " + backQuote(name) + " was specified twice",
                        ErrorCodes::BAD_ARGUMENTS);
                new_temporary_table_names[name] = new_name;
                temporary_table_names.insert(name);
                break;
            }

            case ASTBackupQuery::ALL_DATABASES: break;
            case ASTBackupQuery::ALL_TEMPORARY_TABLES: break;
            case ASTBackupQuery::EVERYTHING: break;
        }
    }
}


const std::unordered_set<String> & RenamingInBackup::getTableNames(const String & database_name) const
{
    auto it = table_names.find(database_name);
    if (it != table_names.end())
        return it->second;
    static const std::unordered_set<String> empty_set;
    return empty_set;
}

DatabaseAndTableName RenamingInBackup::getNewTableName(const DatabaseAndTableName & table_name) const
{
    auto it = new_table_names.find(table_name);
    if (it != new_table_names.end())
        return it->second;
    return {getNewDatabaseName(table_name.first), table_name.second};
}

const std::unordered_set<String> & RenamingInBackup::getDatabaseNames() const
{
    return database_names;
}

const String & RenamingInBackup::getNewDatabaseName(const String & database_name) const
{
    auto it = new_database_names.find(database_name);
    if (it != new_database_names.end())
        return it->second;
    return database_name;
}

const std::unordered_set<String> & RenamingInBackup::getTemporaryTableNames() const
{
    return temporary_table_names;
}

const String & RenamingInBackup::getNewTemporaryTableName(const String & temporary_table_name) const
{
    auto it = new_temporary_table_names.find(temporary_table_name);
    if (it != new_temporary_table_names.end())
        return it->second;
    return temporary_table_name;
}

}
