#pragma once

#include <Parsers/IAST.h>
#include <Backup/RestoreMode.h>


namespace DB
{
using Strings = std::vector<String>;


/** BACKUP [DIFFERENCES SINCE 'base_backup_name' IN]
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
class ASTBackupQuery : public IAST
{
public:
    enum class Kind
    {
        BACKUP,
        RESTORE,
    };
    Kind kind = Kind::BACKUP;

    bool all_databases = false;

    struct DatabaseInfo
    {
        String database_name;
        String new_database_name;
    };

    std::vector<DatabaseInfo> databases;

    struct TableInfo
    {
        String database_name;
        String table_name;
        String new_database_name;
        String new_table_name;
        Strings partitions;
    };

    std::vector<TableInfo> tables;

    String backup_name;

    bool use_incremental_backup = false;
    String base_backup_name;

    RestoreMode restore_mode = RestoreMode::FROM_SCRATCH;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & format, FormatState &, FormatStateStacked) const override;
};
}
