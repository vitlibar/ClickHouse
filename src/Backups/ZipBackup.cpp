#include <Backups/ZipBackup.h>
#include <Disks/IDisk.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/ZipArchiveReader.h>
#include <IO/ZipArchiveWriter.h>


namespace DB
{
ZipBackup::ZipBackup(
    const String & backup_name_,
    const DiskPtr & disk_,
    const String & path_,
    const ContextPtr & context_,
    const std::optional<BackupInfo> & base_backup_info_)
    : BackupImpl(backup_name_, context_, base_backup_info_), disk(disk_), path(path_)
{
}

ZipBackup::~ZipBackup()
{
    close();
}

bool ZipBackup::backupExists() const
{
    return disk ? disk->exists(path) : fs::exists(path);
}

void ZipBackup::openImpl(OpenMode open_mode_)
{
    /// mutex is already locked
    if (open_mode_ == OpenMode::WRITE)
    {
        if (disk)
            writer = ZipArchiveWriter::create(disk->writeFile(path), path);
        else
            writer = ZipArchiveWriter::create(path);

        writer->setCompression(compression_method, compression_level);
        writer->setPassword(password);
    }
    else if (open_mode_ == OpenMode::READ)
    {
        if (disk)
        {
            auto archive_read_function = [d = disk, p = path]() -> std::unique_ptr<SeekableReadBuffer> { return d->readFile(p); };
            size_t archive_size = disk->getFileSize(path);
            reader = ZipArchiveReader::create(archive_read_function, archive_size, path);
        }
        else
            reader = ZipArchiveReader::create(path);

        reader->setPassword(password);
    }
}

void ZipBackup::closeImpl(bool writing_finalized_)
{
    /// mutex is already locked
    if (writer && writer->isWritingFile())
        throw Exception("There is some writing unfinished on close", ErrorCodes::LOGICAL_ERROR);

    writer.reset();
    reader.reset();

    if ((getOpenModeNoLock() == OpenMode::WRITE) && !writing_finalized_)
        fs::remove(path);
}

std::unique_ptr<ReadBuffer> ZipBackup::readFileImpl(const String & file_name) const
{
    /// mutex is already locked
    return reader->readFile(file_name);
}

std::unique_ptr<WriteBuffer> ZipBackup::addFileImpl(const String & file_name)
{
    /// mutex is already locked
    return writer->writeFile(file_name);
}

void ZipBackup::setCompression(CompressionMethod method_, int level_)
{
    std::lock_guard lock{mutex};
    compression_method = method_;
    compression_level = level_;
    if (writer)
        writer->setCompression(compression_method, compression_level);
}

void ZipBackup::setCompression(const String & method_, int level_)
{
    setCompression(ZipArchiveWriter::parseCompressionMethod(method_), level_);
}

void ZipBackup::setPassword(const String & password_)
{
    std::lock_guard lock{mutex};
    password = password_;
    if (writer)
        writer->setPassword(password);
    if (reader)
        reader->setPassword(password);
}

}
