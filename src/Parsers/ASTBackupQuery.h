#pragma once

#include <Parsers/IAST.h>


namespace DB
{
using Strings = std::vector<String>;
using DatabaseAndTableName = std::pair<String, String>;


/** BACKUP { TABLE [db.]table_name [USING NAME [db.]table_name_in_backup] [PARTITION partition_expr [,...]] |
  *          DICTIONARY [db.]dictionary_name [USING NAME [db.]dictionary_name_in_backup] |
  *          DATABASE database_name [USING NAME database_name_in_backup] |
  *          ALL DATABASES |
  *          TEMPORARY TABLE table_name [USING NAME table_name_in_backup]
  *          ALL TEMPORARY TABLES |
  *          EVERYTHING } [,...]
  *        [WITH BASE 'base_backup_name']
  *        TO 'backup_name'
  *
  * RESTORE { TABLE [db.]table_name [USING NAME [db.]table_name_in_backup] [PARTITION partition_expr [,...]] |
  *           DICTIONARY [db.]dictionary_name [USING NAME [db.]dictionary_name_in_backup] |
  *           DATABASE database_name [USING NAME database_name_in_backup] |
  *           ALL DATABASES |
  *           TEMPORARY TABLE table_name [USING NAME table_name_in_backup] |
  *           ALL TEMPORARY TABLES |
  *           EVERYTHING } [,...]
  *         FROM 'backup_name'
  *
  * Notes:
  * RESTORE doesn't drop any data, it either creates a table or appends an existing table with restored data.
  * This behaviour can cause data duplication.
  * If appending isn't possible because the existing table has incompatible format then RESTORE will throw an exception.
  *
  * The "USING NAME" clause allows to set another the name in the backup for an object while executing BACKUP or RESTORE.
  * Those clauses are useful to backup or restore under another name.
  *
  * "ALL DATABASES" means all databases except the system database and the internal database containing temporary tables.
  * "EVERYTHING" works exactly as "ALL DATABASES, ALL TEMPORARY TABLES"
  *
  * The "WITH BASE" clause allows to set a base backup. Only differences made after the base backup will be
  * included in a newly created backup, so this option allows to make an incremental backup.
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

    enum ElementType
    {
        TABLE,
        DICTIONARY,
        DATABASE,
        ALL_DATABASES,
        TEMPORARY_TABLE,
        ALL_TEMPORARY_TABLES,
        EVERYTHING,
    };

    struct Element
    {
        ElementType type;
        DatabaseAndTableName name;
        DatabaseAndTableName name_in_backup;
        std::set<String> partitions;
        std::set<String> except_list;
    };

    using Elements = std::vector<Element>;
    Elements elements;

    String backup_name;
    String base_backup_name;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
