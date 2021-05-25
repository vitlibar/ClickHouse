#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * BACKUP { TABLE [db.]table_name [USING NAME [db.]table_name_in_backup] [PARTITION partition_expr [,...]] |
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
  */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
