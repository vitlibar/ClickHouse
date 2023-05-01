#pragma once

#include <Backups/BackupEntryWithChecksumCalculation.h>


namespace DB
{

/// Represents small preloaded data to be included in a backup.
class BackupEntryFromMemory : public BackupEntryWithChecksumCalculation<IBackupEntry>
{
public:
    /// The constructor is allowed to not set `checksum_`, in that case it will be calculated from the data.
    BackupEntryFromMemory(const void * data_, size_t size_);
    explicit BackupEntryFromMemory(String data_);

    std::unique_ptr<SeekableReadBuffer> getReadBuffer() const override;
    UInt64 getSize() const override { return data.size(); }

    DataSourceDescription getDataSourceDescription() const override { return DataSourceDescription{DataSourceType::RAM, "", false, false}; }

private:
    const String data;
};

}
