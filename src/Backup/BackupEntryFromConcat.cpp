#include <Backup/BackupEntryFromConcat.h>
#include <IO/ConcatReadBuffer.h>


namespace DB
{
BackupEntryFromConcat::BackupEntryFromConcat(
    const String & path_in_backup_,
    std::unique_ptr<IBackupEntry> first_source_,
    std::unique_ptr<IBackupEntry> second_source_,
    const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_)
    , first_source(std::move(first_source_))
    , second_source(std::move(second_source_))
    , checksum(checksum_)
{
}

UInt64 BackupEntryFromConcat::getSize()
{
    if (!size)
        size = first_source->getSize() + second_source->getSize();
    return *size;
}

std::unique_ptr<ReadBuffer> BackupEntryFromConcat::getReadBuffer()
{
    return std::make_unique<ConcatReadBuffer>(first_source->getReadBuffer(), second_source->getReadBuffer());
}
}
