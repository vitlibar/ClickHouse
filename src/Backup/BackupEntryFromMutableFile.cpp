#include <Backup/BackupEntryFromMutableFile.h>
#include <Common/filesystemHelpers.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/copyData.h>
#include <IO/createReadBufferFromFileBase.h>


namespace DB
{

namespace
{
    /// It's the maximum size of the file to store it in RAM.
    /// If the file is larger than this value a disk copy will be made.
    constexpr size_t MAX_SIZE_TO_STORE_IN_MEMORY = 1024;
}


BackupEntryFromMutableFile::BackupEntryFromMutableFile(
    const String & path_in_backup_, const String & file_path_, const std::optional<UInt64> & file_size_, const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_ ? *file_size_ : Poco::File(file_path_).getSize(), checksum_)
{
    if (getDataSize() <= MAX_SIZE_TO_STORE_IN_MEMORY)
    {
        data.resize(getDataSize());
        createReadBufferFromFileBase(file_path_, 0, 0, 0)->readStrict(data.data(), data.size());
    }
    else
    {
        temp_file = createTemporaryFile("");
        Poco::File(file_path_).copyTo(temp_file->path());
    }
}

BackupEntryFromMutableFile::~BackupEntryFromMutableFile() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromMutableFile::getReadBuffer() const
{
    if (temp_file)
        return createReadBufferFromFileBase(temp_file->path(), 0, 0, 0);
    return std::make_unique<ReadBufferFromString>(data);
}


BackupEntryFromMutableFileOnDisk::BackupEntryFromMutableFileOnDisk(
    const String & path_in_backup_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_)
    : IBackupEntry(path_in_backup_, file_size_ ? *file_size_ : disk_->getFileSize(file_path_), checksum_)
{
    if (getDataSize() <= MAX_SIZE_TO_STORE_IN_MEMORY)
    {
        data.resize(getDataSize());
        disk_->readFile(file_path_)->readStrict(data.data(), data.size());
    }
    else
    {
        temp_file = createTemporaryFile("");
        auto in = disk_->readFile(file_path_);
        auto out = std::make_unique<WriteBufferFromFile>(temp_file->path());
        copyData(*in, *out);
    }
}

BackupEntryFromMutableFileOnDisk::~BackupEntryFromMutableFileOnDisk() = default;

std::unique_ptr<ReadBuffer> BackupEntryFromMutableFileOnDisk::getReadBuffer() const
{
    if (temp_file)
        return createReadBufferFromFileBase(temp_file->path(), 0, 0, 0);
    return std::make_unique<ReadBufferFromString>(data);
}

}
