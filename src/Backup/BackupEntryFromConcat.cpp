#include <Backup/BackupEntryFromConcat.h>
#include <IO/ConcatReadBuffer.h>


namespace DB
{
BackupEntryFromConcat::BackupEntryFromConcat(
    const String & path_in_backup_,
    std::unique_ptr<IBackupEntry> first_source_,
    std::unique_ptr<IBackupEntry> second_source_,
    const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_), first_source(std::move(first_source_)), second_source(std::move(second_source_)), checksum(checksum_)
{
}

std::unique_ptr<ReadBuffer> BackupEntryFromConcat::getReadBuffer() const
{
    return std::make_unique<ConcatReadBuffer>(first_source->getReadBuffer(), second_source->getReadBuffer());
}

UInt64 BackupEntryFromConcat::getDataSize() const
{
    return first_source->getDataSize() + second_source->getDataSize();
}

UInt128 BackupEntryFromConcat::getChecksum() const
{
    if (!checksum)
        checksum = calculateChecksum(getReadBuffer());
    return *checksum;
}

}
