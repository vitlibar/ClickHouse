#pragma once

#include <Parsers/IAST.h>


namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;


/** BACKUP {TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...]] |
  *         DICTIONARY [db.]dictionary_name |
  *         DATABASE database_name [AS new_database_name] |
  *         ALL DATABASES EXCEPT SYSTEM } [,...]
  *        [WITH BASE 'base_backup_name']
  *        TO 'backup_name'
  *
  * RESTORE [{TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...] |
  *           DICTIONARY [db.]dictionary_name [AS db.new_dictionary_name] |
  *           DATABASE database_name [AS new_database_name] } [,...]
  *         [WITH REPLACE IF {TABLE|DATABASE} EXISTS]
  *         FROM 'backup_name'
  *
  * Notes:
  * "WITH BASE" allows to specify the base backup, only differences made after
  * the base backup will be included in a newly created backup.
  *
  * "RESTORE" without specifying objects to restore will restore all the contents
  * of the backup.
  *
  * If some of restored tables exists then "RESTORE" will only add restored data
  * into those tables.
  * "WITH REPLACE IF TABLE EXISTS" allows to specify that restored tables should
  * completely replace the tables which currently exist.
  * "WITH REPLACE IF DATABASE EXISTS" allows to specify that restored databases should
  * completely replace the databases which currently exist.
  */
class ASTBackupQuery : public IAST
{
public:
    enum class Kind
    {
        BACKUP,
        RESTORE,
    };
    Kind kind = Kind::BACKUP;

    struct TableInfo
    {
        DatabaseAndTableName table_name;
        DatabaseAndTableName new_table_name;
        Strings partitions;
    };

    std::vector<TableInfo> tables;

    struct DictionaryInfo
    {
        DatabaseAndTableName dictionary_name;
        DatabaseAndTableName new_dictionary_name;
    };

    std::vector<DictionaryInfo> dictionaries;

    struct DatabaseInfo
    {
        String database_name;
        String new_database_name;
    };

    std::vector<DatabaseInfo> databases;

    bool all_databases = false;

    String backup_name;
    String base_backup_name;

    bool replace_table_if_exists = false;
    bool replace_database_if_exists = false;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
