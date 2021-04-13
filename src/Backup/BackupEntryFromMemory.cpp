#include <Backup/BackupEntryFromMemory.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

BackupEntryFromMemory::BackupEntryFromMemory(const String & path_in_backup_, const void * data_, size_t size_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(path_in_backup_, String{reinterpret_cast<const char *>(data_), size_}, checksum_)
{
}

BackupEntryFromMemory::BackupEntryFromMemory(const String & path_in_backup_, String data_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_), data(std::move(data_)), checksum(checksum_)
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromMemory::getReadBuffer() const
{
    return std::make_unique<ReadBufferFromString>(data);
}

UInt64 BackupEntryFromMemory::getDataSize() const
{
    return data.size();
}

UInt128 BackupEntryFromMemory::getChecksum() const
{
    if (!checksum)
    {
        auto u128 = CityHash_v1_0_2::CityHash128WithSeed(data.data(), data.size(), {0, 0});
        checksum = UInt128{u128.first, u128.second};
    }
    return *checksum;
}

std::optional<UInt128> BackupEntryFromMemory::tryGetChecksumFast() const
{
    return checksum;
}

}
