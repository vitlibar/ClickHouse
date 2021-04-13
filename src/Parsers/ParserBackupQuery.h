#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** Parses queries like
  * BACKUP [DIFFERENCES SINCE 'base_backup_name' IN]
  *        {ALL DATABASES |
  *         DATABASE database_name |
  *         TABLE [db.]table_name [PARTITION partition_expr [,...]]} [,...]
  *        TO 'backup_name'
  *
  * RESTORE [{DATABASE database_name [AS new_database_name] |
  *           TABLE [db.]table_name [AS db.new_table_name] [PARTITION partition_expr [,...]]} [,...]]
  *         FROM 'backup_name'
  *         [FROM SCRATCH | REPLACE OLD DATA | KEEP OLD DATA]
  */
class ParserBackupQuery : public IParserBase
{
protected:
    const char * getName() const override { return "BACKUP or RESTORE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
