#include <Backup/BackupSnapshotFromFile.h>
#include <Backup/BackupEntry.h>
#include <Common/escapeForFileName.h>
#include <Disks/IDisk.h>
#include <IO/HashingReadBuffer.h>
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

    UInt128 calculateChecksum(const String & data)
    {
        auto u128 = CityHash_v1_0_2::CityHash128WithSeed(data.data(), data.size(), {0, 0});
        return UInt128{u128.first, u128.second};
    }

    UInt128 calculateChecksum(ReadBuffer & buffer)
    {
        HashingReadBuffer hashing_buffer(buffer);
        hashing_buffer.ignoreAll();
        auto u128 = hashing_buffer.getHash();
        return UInt128{u128.first, u128.second};
    }

    /// For PossibleChanges::ANY it's the maximum size of the file to store it in RAM.
    /// If the file is larger than this value a disk copy will be made.
    constexpr size_t MAX_BYTES_TO_STORE_IN_RAM = 1024;
}


BackupSnapshotFromFile::BackupSnapshotFromFile(
    const String & name_,
    const DiskPtr & disk_,
    const String & file_path_,
    const std::optional<UInt128> & checksum_,
    PossibleChanges possible_changes_,
    const BackupSnapshotParams & params_)
    : name(name_), disk(disk_), possible_changes(possible_changes_), calculate_checksum(params_.calculate_checksums), checksum(checksum_)
{
    if (!params_.directory_for_temp_files.ends_with('/'))
        throw Exception("Directory for temp files should end with '/'", ErrorCodes::BAD_ARGUMENTS);

    switch (possible_changes)
    {
        case PossibleChanges::NONE:
        {
            temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
            disk->createHardLink(file_path_, temp_file_path);
            break;
        }

        case PossibleChanges::APPEND:
        {
            data_size = disk->getFileSize(file_path_);
            reading_size_is_limited = true;
            temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
            disk->createHardLink(file_path_, temp_file_path);
            break;
        }

        case PossibleChanges::ANY:
        {
            data_size = disk->getFileSize(file_path_);
            if (*data_size <= MAX_BYTES_TO_STORE_IN_RAM)
            {
                data.emplace();
                data->resize(*data_size);
                disk->readFile(file_path_)->readStrict(data->data(), data->size());
            }
            else
            {
                temp_file_path = generateTempFilePath(params_.directory_for_temp_files, file_path_);
                disk->copy(file_path_, disk, temp_file_path);
            }
            break;
        }

        default:
            throw Exception("Unexpected PossibleChanges", ErrorCodes::BAD_ARGUMENTS);
    }
}


BackupSnapshotFromFile::~BackupSnapshotFromFile()
{
    if (!temp_file_path.empty())
        disk->removeFile(temp_file_path);
}


bool BackupSnapshotFromFile::getNextEntry(BackupEntry & entry)
{
    if (backup_entry_generated)
        return false;

    std::unique_ptr<ReadBuffer> read_buffer;
    if (data)
    {
        read_buffer = std::make_unique<ReadBufferFromString>(*data);
        if (!data_size)
            data_size = data->size();
        if (!checksum && calculate_checksum)
            checksum = calculateChecksum(*data);
    }
    else
    {
        std::unique_ptr<ReadBufferFromFileBase> buffer_from_file = disk->readFile(temp_file_path);
        if (!data_size)
            data_size = disk->getFileSize(temp_file_path);

        if (!checksum && calculate_checksum)
        {
            if (reading_size_is_limited)
            {
                LimitReadBuffer limit_buffer{*buffer_from_file, *data_size, true};
                checksum = calculateChecksum(limit_buffer);
            }
            else
                checksum = calculateChecksum(*buffer_from_file);
            buffer_from_file->seek(0, SEEK_SET);
        }

        if (reading_size_is_limited)
            read_buffer = std::make_unique<LimitReadBuffer>(std::move(buffer_from_file), *data_size, true);
        else
            read_buffer = std::move(buffer_from_file);
    }

    entry = {};
    entry.name = name;
    entry.data_size = *data_size;
    entry.checksum = checksum;
    entry.read_buffer = std::move(read_buffer);

    backup_entry_generated = true;
    return true;
}

}
