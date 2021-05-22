#pragma once

#include <Parsers/IAST.h>
#include <Backup/RestoreWithReplaceMode.h>


namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;
using DatabaseAndDictionaryName = std::pair<String, String>;


/** BACKUP {TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...]] |
  *         DICTIONARY [db.]dictionary_name |
  *         DATABASE database_name [AS new_database_name] |
  *         ALL DATABASES |
  *         TEMPORARY TABLE table_name [AS new_table_name] |
  *         ALL TEMPORARY TABLES |
  *         EVERYTHING} [,...]
  *        [WITH BASE 'base_backup_name']
  *        TO 'backup_name'
  *
  * RESTORE [{TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...] |
  *           DICTIONARY [db.]dictionary_name [AS db.new_dictionary_name] |
  *           DATABASE database_name [AS new_database_name] |
  *           ALL DATABASES |
  *           TEMPORARY TABLE table_name [AS new_table_name] |
  *           ALL TEMPORARY TABLES
  *           EVERYTHING} [,...]
  *         [WITH REPLACE IF {TABLE|DATABASE} EXISTS]
  *         FROM 'backup_name'
  *
  * Notes:
  * "WITH BASE" allows to specify the base backup, only differences made after
  * the base backup will be included in a newly created backup.
  *
  * "RESTORE" without specifying objects to restore works exactly as "RESTORE EVERYTHING", i.e. it restores
  * all the contents of the backup.
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
    enum Kind
    {
        BACKUP,
        RESTORE,
    };
    Kind kind = Kind::BACKUP;

    enum EntryType
    {
        TABLE,
        DICTIONARY,
        DATABASE,
        ALL_DATABASES,
        TEMPORARY_TABLE,
        ALL_TEMPORARY_TABLES,
        EVERYTHING,
    };

    struct Entry
    {
        EntryType type;
        DatabaseAndTableName name;
        DatabaseAndTableName new_name;
        Strings partitions;
    };

    std::vector<Entry> entries;

    String backup_name;
    String base_backup_name;

    RestoreWithReplaceMode replace_mode;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
