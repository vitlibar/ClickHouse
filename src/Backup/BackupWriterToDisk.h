#pragma once

#include <Core/Types.h>
#include <Backup/IBackupWriter.h>
#include <mutex>
#include <unordered_map>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;
class IBackupReader;
using BackupReaderPtr = std::shared_ptr<const IBackupReader>;


class BackupWriterToDisk : public IBackupWriter
{
public:
    BackupWriterToDisk(const DiskPtr & disk_, const String & directory_, const BackupReaderPtr & incremental_base_);
    ~BackupWriterToDisk() override;

    void write(BackupEntry && entry) override;

private:
    struct Entry
    {
        UInt128 checksum;
        bool is_symlink = false;

        /// For incremental backups. This is an index in the vector `incremental_bases`, 0 means this backup.
        size_t incremental_base_index = 0;
    };

    const String directory;
    std::vector<BackupReaderPtr> incremental_bases;
    std::unordered_map<String, Entry> entries;
    std::mutex mutex;
};

}
