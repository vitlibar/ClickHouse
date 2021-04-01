#include <Backup/BackupSnapshotFromFile.h>
#include <Backup/BackupEntry.h>
#include <Common/createHardLink.h>
#include <Common/escapeForFileName.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


namespace
{
    String generateTempFilePath(const String & directory_for_temp_files,
                                const String & entry_name)
    {
        return directory_for_temp_files + escapeForFileName(entry_name);
    }

    /// For PossibleChanges::ANY it's the maximum size of the file to store it in RAM.
    /// If the file is larger than this value a disk copy will be made.
    constexpr size_t MAX_BYTES_TO_STORE_IN_RAM = 1024;
}

BackupSnapshotFromFile::BackupSnapshotFromFile(
    const String & name_,
    const String & file_path_,
    const std::optional<UInt128> & checksum_,
    PossibleChanges possible_changes_,
    const BackupSnapshotParams & params_)
    : name(name_), possible_changes(possible_changes_), checksum(checksum_)
{
    if (!params_.directory_for_temp_files.ends_with('/') && !params_.directory_for_temp_files.empty())
        throw Exception("Directory for temp files should end with '/'", ErrorCodes::BAD_ARGUMENTS);

    switch (possible_changes)
    {
        case PossibleChanges::NONE:
        {
            temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
            utilCreateHardLink(file_path_, temp_file_path);
            break;
        }

        case PossibleChanges::APPEND:
        {
            data_size = utilGetFileSize(file_path_);
            reading_size_is_limited = true;
            temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
            utilCreateHardLink(file_path_, temp_file_path);
            break;
        }

        case PossibleChanges::ANY:
        {
            data_size = utilGetFileSize(file_path_);
            if (*data_size <= MAX_BYTES_TO_STORE_IN_RAM)
            {
                data.emplace();
                data->resize(*data_size);
                utilReadFile(file_path_)->readStrict(data->data(), data->size());
            }
            else
            {
                temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
                utilCopyFile(file_path_, temp_file_path);
            }
            break;
        }

        default:
            throw Exception("Unexpected PossibleChanges", ErrorCodes::BAD_ARGUMENTS);
    }
}


BackupSnapshotFromFile::~BackupSnapshotFromFile()
{
    close();
    if (!temp_file_path.empty())
        utilRemoveFile(temp_file_path);
}


bool BackupSnapshotFromFile::getNextEntry(BackupEntry & entry)
{
    if (backup_entry_generated)
        return false;

    if (!data_size)
    {
        if (data)
            data_size = data->size();
        else
            data_size = utilGetFileSize(temp_file_path);
    }

    entry = {};
    entry.name = name;
    entry.checksum = checksum;
    entry.data_size = *data_size;

    entry.get_read_buffer_function = [this]() -> std::unique_ptr<ReadBuffer>
    {
        if (data)
            return std::make_unique<ReadBufferFromString>(*data);

        std::unique_ptr<ReadBuffer> read_buffer = utilReadFile(temp_file_path);
        if (reading_size_is_limited)
            read_buffer = std::make_unique<LimitReadBuffer>(std::move(read_buffer), *data_size, false);
        return read_buffer;
    };

    backup_entry_generated = true;
    return true;
}

void BackupSnapshotFromFile::utilCopyFile(const String & src_file_path, const String & dest_file_path) const
{
    disk->copy(src_file_path, disk, dest_file_path);
}

void BackupSnapshotFromFile::utilCreateHardLink(const String & src_file_path, const String & dest_file_path) const
{
    disk->createHardLink(src_file_path, dest_file_path);
}

void BackupSnapshotFromFile::utilRemoveFile(const String & file_path) const
{
    disk->removeFile(file_path);
}

size_t BackupSnapshotFromFile::utilGetFileSize(const String & file_path) const
{
    return disk->getFileSize(file_path);
}

std::unique_ptr<ReadBuffer> BackupSnapshotFromFile::utilReadFile(const String & file_path) const
{
    return disk->readFile(file_path);
}

}
