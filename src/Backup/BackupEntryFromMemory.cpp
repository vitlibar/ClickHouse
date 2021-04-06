#include <Backup/BackupEntryFromMemory.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

BackupEntryFromMemory::BackupEntryFromMemory(const String & path_in_backup_, const void * data_, size_t size_, const std::optional<UInt128> & checksum_)
    : BackupEntryFromMemory(path_in_backup_, String{reinterpret_cast<const char *>(data_), size_}, checksum_)
{
}

BackupEntryFromMemory::BackupEntryFromMemory(const String & path_in_backup_, String data_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, data_.size(), checksum_), data(std::move(data_))
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromMemory::getReadBuffer() const
{
    return std::make_unique<ReadBufferFromString>(data);
}

}
