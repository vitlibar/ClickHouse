#include <Backup/BackupWriterToDisk.h>
#include <Backup/IBackupReader.h>


namespace DB
{
BackupWriterToDisk::BackupWriterToDisk(const DiskPtr & disk_, const String & directory_, const BackupReaderPtr & incremental_base_)
    : disk(disk_), directory(directory_)
{
    if (incremental_base_)
    {
        auto base = incremental_base_;
        while(base)
        {
            incremental_bases.push_back(base);
            base = base->getIncrementalBase();
        }
    }
}


void BackupWriterToDisk::write(BackupEntry && entry)
{
}

}
