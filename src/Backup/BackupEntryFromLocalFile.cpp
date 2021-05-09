#include <Backup/BackupEntryFromLocalFile.h>
#include <Common/createHardLink.h>
#include <Common/filesystemHelpers.h>
#include <Common/LRUCache.h>
#include <Disks/DiskLocal.h>
#include <Disks/IVolume.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/createReadBufferFromFileBase.h>
#include <Poco/Path.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NO_TEMP_DIRECTORY;
}

namespace
{
    /// Directory for temporary backup files relative to the temp directory.
    constexpr char TEMP_BACKUPS_DIRECTORY[] = "backups/";

    /// It's the maximum size of the file to keep it in RAM.
    /// If the file is larger than this value a disk copy will be made.
    constexpr size_t MAX_SIZE_FOR_PRELOADING = 1024;
}


BackupEntryFromLocalFile::BackupEntryFromLocalFile(
    const String & path_in_backup_,
    const String & file_path_,
    Flags flags_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const VolumePtr & temporary_volume_)
    : IBackupEntry(path_in_backup_)
    , file_path(file_path_)
    , flags(flags_)
    , temporary_volume(temporary_volume_)
    , file_size(file_size_)
    , checksum(checksum_)
{
    init();
}

BackupEntryFromLocalFile::BackupEntryFromLocalFile(
    const String & path_in_backup_,
    const std::shared_ptr<DiskLocal> & disk_,
    const String & file_path_,
    Flags flags_,
    const std::optional<UInt64> & file_size_,
    const std::optional<UInt128> & checksum_,
    const VolumePtr & temporary_volume_,
    const String & temp_directory_on_disk_)
    : IBackupEntry(path_in_backup_)
    , file_path(fullPath(disk_, file_path_))
    , flags(flags_)
    , temporary_volume(temporary_volume_)
    , temp_directory(temp_directory_on_disk_.empty() ? "" : fullPath(disk_, temp_directory_on_disk_))
    , file_size(file_size_)
    , checksum(checksum_)
{
    init();
}

BackupEntryFromLocalFile::~BackupEntryFromLocalFile() = default;

void BackupEntryFromLocalFile::init()
{
    if ((static_cast<size_t>(flags & Flags::MUTABLE) + static_cast<size_t>(flags & Flags::IMMUTABLE)
             + static_cast<size_t>(flags & Flags::APPEND_ONLY) != 1)
        || (static_cast<size_t>(flags & Flags::ALWAYS_EXISTS) + static_cast<size_t>(flags & Flags::REMOVABLE) != 1))
    {
        throw Exception("Invalid flags passed to BackupEntryFromFile", ErrorCodes::BAD_ARGUMENTS);
    }

    if (!temp_directory.ends_with('/') && !temp_directory.empty())
        throw Exception("Temp directory should end with '/'", ErrorCodes::BAD_ARGUMENTS);

    if (flags & Flags::MUTABLE)
    {
        if (!file_size)
            file_size = Poco::File(file_path).getSize();
        if (*file_size <= MAX_SIZE_FOR_PRELOADING)
        {
            data.emplace();
            data->resize(*file_size);
            auto read_buffer = createReadBufferFromFileBase(file_path, 0, 0, 0);
            read_buffer->readStrict(data->data(), data->size());
            read_buffer->ignoreAll();
            throw Exception(
                "Unexpected file size: " + std::to_string(read_buffer->getPosition()) + " != " + std::to_string(*file_size),
                ErrorCodes::BAD_ARGUMENTS);
        }
        else
        {
            createTemporaryFile({});
            Poco::File(file_path).copyTo(temporary_file->path());
            if (temporary_file->getSize() != *file_size)
            {
                throw Exception(
                    "Unexpected file size: " + std::to_string(temporary_file->getSize()) + " != " + std::to_string(*file_size),
                    ErrorCodes::BAD_ARGUMENTS);
            }
        }
        return;
    }

    if (flags & Flags::ALWAYS_EXISTS)
    {
        if (flags & Flags::APPEND_ONLY)
        {
            if (!file_size)
                file_size = Poco::File(file_path).getSize();
        }
        return;
    }

    if (flags & Flags::REMOVABLE)
    {
        auto st = getStat(file_path);
        if (file_size && (file_size != st.st_size))
        {
            throw Exception("Unexpected file size: " + std::to_string(*file_size),
                            ErrorCodes::BAD_ARGUMENTS);
        }
        file_size = st.st_size;
        createTemporaryFile(st.st_dev);
        createHardLink(temporary_file->path(), file_path);
        return;
    }
}

void BackupEntryFromLocalFile::createTemporaryFile(const std::optional<dev_t> & device_id)
{
    if (!device_id)
    {
        if (temporary_volume)
        {
            temporary_file = ::DB::createTemporaryFile(temporary_volume->getDisk()->getPath() + TEMP_BACKUPS_DIRECTORY);
            return;
        }

        if (!temp_directory.empty())
        {
            temporary_file = ::DB::createTemporaryFile(temp_directory + TEMP_BACKUPS_DIRECTORY);
            return;
        }

        throw Exception("Cannot find a temp directory", ErrorCodes::NO_TEMP_DIRECTORY);
    }

    static LRUCache<dev_t, String> cache{32};
    auto cache_element = cache.get(*device_id);
    if (cache_element)
    {
        try
        {
            const String & temp_dir = *cache_element;
            if (getStat(temp_dir).st_dev == device_id)
            {
                temporary_file = ::DB::createTemporaryFile(temp_dir + TEMP_BACKUPS_DIRECTORY);
                return;
            }
        }
        catch(...)
        {
        }
    }

    auto get_device_id = [](const String & temp_directory)
    {
        std::filesystem::path p = std::filesystem::weakly_canonical(temp_directory);

        while (p.has_relative_path() && !std::filesystem::exists(p))
            p = p.parent_path();

        return getStat(p).st_dev;
    };

    if (temporary_volume)
    {
        for (const auto & temp_disk : temporary_volume->getDisks())
        {
            const String & temp_disk_path = temp_disk->getPath();
            if (get_device_id(temp_disk_path) == *device_id)
            {
                cache.set(*device_id, std::make_shared<String>(temp_disk_path));
                temporary_file = ::DB::createTemporaryFile(temp_disk_path + TEMP_BACKUPS_DIRECTORY);
                return;
            }
        }
    }

    if (!temp_directory.empty() && (get_device_id(temp_directory) == *device_id))
    {
        cache.set(*device_id, std::make_shared<String>(temp_directory));
        temporary_file = ::DB::createTemporaryFile(temp_directory + TEMP_BACKUPS_DIRECTORY);
        return;
    }

    throw Exception(
        "Cannot find a temp directory in the file system mounted at " + getMountPoint(file_path).string(), ErrorCodes::NO_TEMP_DIRECTORY);
}

std::unique_ptr<ReadBuffer> BackupEntryFromLocalFile::getReadBuffer() const
{
    if (data)
        return std::make_unique<ReadBufferFromString>(*data);

    if (temporary_file)
    {
        if (flags & Flags::APPEND_ONLY)
            return std::make_unique<LimitReadBuffer>(createReadBufferFromFileBase(temporary_file->path(), 0, 0, 0), *file_size, false);
        else
            return createReadBufferFromFileBase(temporary_file->path(), 0, 0, 0);
    }

    if (flags & Flags::APPEND_ONLY)
        return std::make_unique<LimitReadBuffer>(createReadBufferFromFileBase(file_path, 0, 0, 0), *file_size, false);
    else
        return createReadBufferFromFileBase(file_path, 0, 0, 0);
}

UInt64 BackupEntryFromLocalFile::getDataSize() const
{
    if (!file_size)
    {
        if (data)
            file_size = data->size();
        else if (temporary_file)
            file_size = temporary_file->getSize();
        else
            file_size = Poco::File(file_path).getSize();
    }
    return *file_size;
}

UInt128 BackupEntryFromLocalFile::getChecksum() const
{
    if (!checksum)
        checksum = calculateChecksum(getReadBuffer());
    return *checksum;
}

}
