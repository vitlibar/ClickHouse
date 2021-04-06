#include <Databases/IDatabase.h>


namespace DB
{

BackupEntries IDatabase::backup(const Context & context) const
{

}

void IDatabase::restore(const IBackup & backup, const Context & context)
{

}

BackupEntries IDatabase::backupTable(const String & table_name, const Context & context) const
{

}

void IDatabase::restoreTable(const IBackup & backup, const String & table_name, const Context & context)
{

}

}
