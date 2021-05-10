#pragma once

#include <Backup/IBackupEntry.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

/// Represents small preloaded data to be included in a backup.
class BackupEntryFromMemory : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromMemory(const String & path_in_backup_, const void * data_, size_t size_, const std::optional<UInt128> & checksum_ = {});
    BackupEntryFromMemory(const String & path_in_backup_, String data_, const std::optional<UInt128> & checksum_ = {});

    UInt64 getSize() override { return data.size(); }
    std::optional<UInt128> getChecksum() override { return checksum; }
    std::unique_ptr<ReadBuffer> getReadBuffer() override;

private:
    const String data;
    std::optional<UInt128> checksum;
};

}
