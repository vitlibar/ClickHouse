#include <Backup/RenamingInBackup.h>
#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

using Kind = ASTBackupQuery::Kind;
using ElementType = ASTBackupQuery::ElementType;


RenamingInBackup::RenamingInBackup(const ASTBackupQuery & query)
{
    for (const auto & element : query.elements)
    {
        switch (element.type)
        {
            case ElementType::TABLE: [[fallthrough]];
            case ElementType::DICTIONARY:
            {
                if (query.kind == Kind::BACKUP)
                    old_to_new_table_names[element.name] = element.name_in_backup;
                else
                    old_to_new_table_names[element.name_in_backup] = element.name;
                break;
            }

            case ASTBackupQuery::DATABASE:
            {
                if (query.kind == Kind::BACKUP)
                    old_to_new_database_names[element.name.first] = element.name_in_backup.first;
                else
                    old_to_new_database_names[element.name_in_backup.first] = element.name.first;
                break;
            }

            case ASTBackupQuery::TEMPORARY_TABLE:
            {
                if (query.kind == Kind::BACKUP)
                    old_to_new_database_names[element.name.second] = element.name_in_backup.second;
                else
                    old_to_new_database_names[element.name_in_backup.second] = element.name.second;
                break;
            }

            case ASTBackupQuery::ALL_DATABASES: break;
            case ASTBackupQuery::ALL_TEMPORARY_TABLES: break;
            case ASTBackupQuery::EVERYTHING: break;
        }
    }
}


DatabaseAndTableName RenamingInBackup::getNewTableName(const DatabaseAndTableName & table_name) const
{
    auto it = old_to_new_table_names.find(table_name);
    if (it != old_to_new_table_names.end())
        return it->second;
    return {getNewDatabaseName(table_name.first), table_name.second};
}

const String & RenamingInBackup::getNewDatabaseName(const String & database_name) const
{
    auto it = old_to_new_database_names.find(database_name);
    if (it != old_to_new_database_names.end())
        return it->second;
    return database_name;
}

const String & RenamingInBackup::getNewTemporaryTableName(const String & temporary_table_name) const
{
    auto it = old_to_new_temporary_table_names.find(temporary_table_name);
    if (it != old_to_new_temporary_table_names.end())
        return it->second;
    return temporary_table_name;
}

ASTPtr RenamingInBackup::getNewCreateQuery(const ASTPtr & create_query) const
{
    const auto & src = typeid_cast<const ASTCreateQuery &>(*create_query);
    auto dst = typeid_cast<std::shared_ptr<ASTCreateQuery>>(src.clone());

    if (dst->table.empty())
        dst->database = getNewDatabaseName(dst->database);
    else if (dst->temporary)
        dst->table = getNewTemporaryTableName(dst->table);
    else
        std::tie(dst->database, dst->table) = getNewTableName({dst->database, dst->table});

    if (!dst->as_table.empty())
        std::tie(dst->as_database, dst->as_table) = getNewTableName({dst->as_database, dst->as_table});

    // TODO(table functions):
    //if (dst->as_table_function)

    // TODO(dictionary with a source from clickhouse)
    if (dst->dictionary && dst->dictionary->source && dictionary->source->name == "clickhouse")
    {

    }

    return dst;
}

}
