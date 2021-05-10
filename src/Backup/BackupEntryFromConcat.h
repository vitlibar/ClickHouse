#pragma once

#include <Backup/IBackupEntry.h>


namespace DB
{

/// Concatenates data of two backup entries.
class BackupEntryFromConcat : public IBackupEntry
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromConcat(
        const String & path_in_backup_,
        std::unique_ptr<IBackupEntry> first_source_,
        std::unique_ptr<IBackupEntry> second_source_,
        const std::optional<UInt128> & checksum_ = {});

    UInt64 getSize() override;
    std::optional<UInt128> getChecksum() override { return checksum; }
    std::unique_ptr<ReadBuffer> getReadBuffer() override;

private:
    std::unique_ptr<IBackupEntry> first_source;
    std::unique_ptr<IBackupEntry> second_source;
    std::optional<UInt64> size;
    std::optional<UInt128> checksum;
};

}
