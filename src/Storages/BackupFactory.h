#pragma once

#include <Storages/IBackup.h>

namespace DB
{
class BackupFactory
{
public:
    using OpenMode = IBackup::OpenMode;
    static BackupFactory & instance();
    std::shared_ptr<IBackup> get(const String & backup_directory_, OpenMode open_mode_, const String & base_backup_directory_);
};

}
