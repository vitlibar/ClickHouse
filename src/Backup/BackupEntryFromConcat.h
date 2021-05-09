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

    std::unique_ptr<ReadBuffer> getReadBuffer() const override;
    UInt64 getDataSize() const override;
    UInt128 getChecksum() const override;
    std::optional<UInt128> tryGetChecksumFast() const override { return checksum; }

private:
    std::unique_ptr<IBackupEntry> first_source;
    std::unique_ptr<IBackupEntry> second_source;
    mutable std::optional<UInt128> checksum;
};

}
