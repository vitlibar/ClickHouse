#pragma once

#include <Backup/IBackup.h>
#include <unordered_map>


namespace DB
{
class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;


class BackupOnDisk : public IBackup
{
public:
    BackupOnDisk(const DiskPtr & disk_, const String & directory_, const std::shared_ptr<const IBackup> & incremental_base_);
    ~BackupOnDisk() override;

    OpenMode getOpenMode() const override;
    Strings list() const override;
    bool exists(const String & path) const override;
    size_t getDataSize(const String & name) const override;
    UInt128 getChecksum(const String & path) const override;
    BackupEntry read(const String & path) const override;
    void write(BackupEntry && entry) override;

private:
    struct Entry
    {
        size_t data_size;
        UInt128 checksum;

        /// For incremental backups. This is an index in the vector `incremental_bases`, 0 means this backup.
        size_t incremental_base_index = 0;
    };

    const DiskPtr disk;
    const String directory;
    std::vector<BackupReaderPtr> incremental_bases;
    std::unordered_map<String, Entry> entries;
};

}
