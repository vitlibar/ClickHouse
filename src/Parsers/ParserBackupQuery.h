#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * BACKUP {TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...]] |
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
  */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
