#include <Backup/BackupOnDisk.h>
#include <Backup/BackupEntryFromConcat.h>
#include <Backup/BackupEntryFromFile.h>
#include <Backup/BackupEntryFromMemory.h>
#include <Backup/IBackupEntry.h>
#include <Common/typeid_cast.h>
#include <Common/quoteString.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <IO/HashingReadBuffer.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Util/XMLConfiguration.h>
#include <ext/range.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BACKUP_VERSION_NOT_SUPPORTED;
    extern const int BACKUP_DIRECTORY_NOT_EMPTY;
    extern const int BACKUP_DAMAGED;
    extern const int BACKUP_ENTRY_EXISTS;
    extern const int BACKUP_ENTRY_NOT_EXISTS;
}

namespace
{
    extern const UInt64 BACKUP_CONTENTS_VERSION = 1;
}


BackupOnDisk::BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_)
    : BackupOnDisk(open_mode_, disk_, directory_, std::shared_ptr<const IBackup>{})
{
}

BackupOnDisk::BackupOnDisk(OpenMode open_mode_, const DiskPtr & disk_, const String & directory_, const std::shared_ptr<const IBackup> & base_backup_)
    : open_mode(open_mode_), disk(disk_), directory(directory_), base_backup(base_backup_)
{
    open();
}

BackupOnDisk::BackupOnDisk(OpenMode open_mode_, const String & disk_name_, const String & directory_, const DiskSelector & disk_selector_)
    : open_mode(open_mode_), disk(disk_selector_.get(disk_name_)), disk_selector(&disk_selector_), directory(directory_)
{
    open();
}

BackupOnDisk::~BackupOnDisk()
{
    close();
}

void BackupOnDisk::open()
{
    if (!directory.ends_with('/') && !directory.empty())
        throw Exception("Directory for backup should end with '/'", ErrorCodes::BAD_ARGUMENTS);

    if (open_mode == OpenMode::CREATE)
    {
        disk->createDirectories(directory);
        directory_was_empty = disk->isDirectoryEmpty(directory);
        if (!directory_was_empty)
            throw Exception("Directory for backup is not empty", ErrorCodes::BACKUP_DIRECTORY_NOT_EMPTY);
        writeLockFile();
        writeBaseBackupInfo();
    }

    if (open_mode == OpenMode::READ)
    {
        readBaseBackupInfo();
        readContents();
    }
}

void BackupOnDisk::close()
{
    if (open_mode == OpenMode::CREATE)
    {
        if (!writing_finished && directory_was_empty)
        {
            /// Creating of the backup wasn't finished correctly,
            /// so the backup cannot be used and it's better to remove its files.
            disk->removeRecursive(directory);
        }
    }
}

void BackupOnDisk::writeLockFile()
{
    if (open_mode == OpenMode::CREATE)
    {
        String path_to_lock_file = directory + ".write_lock";
        disk->createFile(path_to_lock_file);
        lock_file_path = path_to_lock_file;
    }
}

void BackupOnDisk::removeLockFile()
{
    if (!lock_file_path.empty())
        disk->removeFile(directory + ".write_lock");
}

void BackupOnDisk::writeBaseBackupInfo()
{
    /// We write the information about the base backup in XML because
    /// we need it to be readable and editable by admin.
    String file_path = directory + ".base";
    if (!base_backup)
    {
        disk->removeFileIfExists(file_path);
        return;
    }
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration};
    config->setString("disk", base_backup->getDisk());
    config->setString("path", base_backup->getPath());
    std::stringstream ss;
    config->save(ss);
    auto out = disk->writeFile(file_path);
    writeString(ss.str(), *out);
}

void BackupOnDisk::readBaseBackupInfo()
{
    base_backup = nullptr;
    String file_path = directory + ".base";
    if (!disk->exists(file_path))
        return;
    auto in = disk->readFile(file_path);
    String str;
    readStringUntilEOF(str, *in);
    in.reset();
    std::stringstream ss{str};
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration};
    config->load(ss);
    String base_backup_disk_name = config->getString("disk", "");
    String base_backup_path = config->getString("path", "");
    if (!base_backup_disk_name.empty())
        base_backup = std::make_shared<BackupOnDisk>(OpenMode::READ, base_backup_disk_name, base_backup_path, disk_selector);
}

void BackupOnDisk::writeContents()
{
    auto out = disk->writeFile(directory + ".contents");
    writeVarUInt(BACKUP_CONTENTS_VERSION, *out);

    writeVarUInt(infos.size(), *out);
    for (const auto & [path_in_backup, info] : infos)
    {
        writeBinary(path_in_backup, *out);
        writeVarUInt(info.data_size, *out);
        writeBinary(info.checksum, *out);
        writeVarUInt(info.base_data_size, *out);
        if (info.base_data_size && (info.base_data_size != info.data_size))
            writeBinary(info.base_checksum, *out);
    }
}

void BackupOnDisk::readContents()
{
    auto in = disk->readFile(directory + ".contents");
    UInt64 version;
    readVarUInt(version, *in);
    if (version != BACKUP_CONTENTS_VERSION)
        throw Exception("Backup version " + std::to_string(version) + " is not supported",
                        ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED);

    size_t num_infos;
    readVarUInt(num_infos, *in);
    infos.clear();
    for (size_t i : ext::range(num_infos))
    {
        String path_in_backup;
        readBinary(path_in_backup, *in);
        EntryInfo info;
        readVarUInt(info.data_size, *in);
        readBinary(info.checksum, *in);
        readVarUInt(info.base_data_size, *in);
        if (info.base_data_size && (info.base_data_size != info.data_size))
            readBinary(info.base_checksum, *in);
        if (info.base_data_size && (info.base_data_size == info.data_size))
            info.base_checksum = info.checksum;
        infos.emplace(path_in_backup, info);
    }
}

IBackup::OpenMode BackupOnDisk::getOpenMode() const
{
    return open_mode;
}

String BackupOnDisk::getDisk() const
{
    return disk->getName();
}

String BackupOnDisk::getPath() const
{
    return directory;
}

Strings BackupOnDisk::list(const String & prefix) const
{
    if (!prefix.ends_with('/') && !prefix.empty())
        throw Exception("prefix should end with '/'", ErrorCodes::BAD_ARGUMENTS);
    std::lock_guard lock{mutex};
    Strings elements;
    for (auto it = infos.lower_bound(prefix); it != infos.end(); ++it)
    {
        const String & path = it->first;
        if (!path.starts_with(prefix))
            break;
        size_t start_pos = prefix.length();
        size_t end_pos = path.find('/', start_pos);
        std::string_view new_element = std::string_view{path}.substr(start_pos, end_pos - start_pos);
        if (!elements.empty() && (elements.back() == new_element))
            continue;
        elements.push_back(String{new_element});
    }
    return elements;
}

bool BackupOnDisk::exists(const String & path_in_backup) const
{
    std::lock_guard lock{mutex};
    return infos.count(path_in_backup) != 0;
}

size_t BackupOnDisk::getDataSize(const String & path_in_backup) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(path_in_backup);
    if (it == infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);
    return it->second.data_size;
}

UInt128 BackupOnDisk::getChecksum(const String & path_in_backup) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(path_in_backup);
    if (it == infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);
    return it->second.checksum;
}


std::unique_ptr<IBackupEntry> BackupOnDisk::read(const String & path_in_backup) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(path_in_backup);
    if (it == infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);

    const auto & info = it->second;
    if (info.data_size == 0)
        return std::make_unique<BackupEntryFromMemory>(path_in_backup, nullptr, 0, info.checksum);

    if (info.base_data_size == 0)
    {
        return std::make_unique<BackupEntryFromFile>(
            path_in_backup, disk, directory + path_in_backup, BackupEntryFromFile::ALWAYS_EXISTS_IMMUTABLE, info.data_size, info.checksum);
    }

    if (!base_backup)
    {
        throw Exception(
            "Entry is marked as to be read from a base backup, but the base backup is not available", ErrorCodes::BACKUP_DAMAGED);
    }

    auto base_entry = base_backup->read(path_in_backup);
    if (base_entry->getDataSize() != info.base_data_size)
    {
        throw Exception(
            "Entry " + quoteString(path_in_backup) + " has unexpected size in the base backup (" + toString(base_entry->getDataSize())
                + " != " + toString(info.base_data_size) + ")",
            ErrorCodes::BACKUP_DAMAGED);
    }

    if (base_entry->getChecksum() != info.base_checksum)
    {
        throw Exception("Entry " + quoteString(path_in_backup) + " has unexpected checksum in the base backup", ErrorCodes::BACKUP_DAMAGED);
    }

    if (info.data_size == info.base_data_size)
        return base_entry;

    if (info.data_size < info.base_data_size)
    {
        throw Exception(
            "Entry " + quoteString(path_in_backup) + " has its data size less than the one in the base backup (" + toString(info.data_size)
                + " < " + toString(info.base_data_size) + ")",
            ErrorCodes::BACKUP_DAMAGED);
    }

    return std::make_unique<BackupEntryFromConcat>(
        path_in_backup,
        std::move(base_entry),
        std::make_unique<BackupEntryFromFile>(
            path_in_backup,
            disk,
            directory + path_in_backup,
            BackupEntryFromFile::ALWAYS_EXISTS_IMMUTABLE,
            info.data_size - info.base_data_size),
        info.checksum);
}


void BackupOnDisk::write(std::unique_ptr<IBackupEntry> entry)
{
    std::lock_guard lock{mutex};
    const String & path_in_backup = entry->getPathInBackup();
    auto it = infos.find(path_in_backup);
    if (it != infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " already exists in the backup", ErrorCodes::BACKUP_ENTRY_EXISTS);

    EntryInfo info;
    info.data_size = entry->getDataSize();

    std::unique_ptr<ReadBuffer> read_buffer_in_use;
    auto get_read_buffer = [&]() -> ReadBuffer *
    {
        if (!read_buffer_in_use)
            read_buffer_in_use = entry->getReadBuffer();
        return read_buffer_in_use.get();
    };

    auto seek = [&](UInt64 offset)
    {
        ReadBuffer * read_buffer = get_read_buffer();
        SeekableReadBuffer * seekable = typeid_cast<SeekableReadBuffer *>(read_buffer);
        if (seekable)
            seekable->seek(offset, SEEK_SET);
        else
        {
            if (read_buffer->count() > offset)
            {
                read_buffer_in_use = entry->getReadBuffer();
                read_buffer = get_read_buffer();
            }
            if (read_buffer->count() < offset)
                read_buffer->ignore(offset - read_buffer->count());
        }
    };

    bool checksum_calculated = false;
    auto calculate_checksum = [&]() -> UInt128
    {
        if (!checksum_calculated)
        {
            auto checksum = entry->tryGetChecksumFast();
            if (checksum)
                info.checksum = *checksum;
            else
            {
                HashingReadBuffer hashing_buffer{*get_read_buffer()};
                hashing_buffer.ignoreAll();
                auto u128 = hashing_buffer.getHash();
                info.checksum = UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
            }
            checksum_calculated = true;
        }
        return info.checksum;
    };

    auto calculate_partial_checksum = [&](UInt64 limit) -> UInt128
    {
        LimitReadBuffer limit_buffer{*get_read_buffer(), limit, true};
        HashingReadBuffer hashing_buffer{*get_read_buffer()};
        hashing_buffer.ignoreAll();
        auto u128 = hashing_buffer.getHash();
        return UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
    };

    if (!info.data_size)
    {
        info.checksum = entry->getChecksum();
        infos.emplace(path_in_backup, info);
        return;
    }

    if (base_backup && base_backup->exists(path_in_backup))
    {
        size_t base_data_size = base_backup->getDataSize(path_in_backup);
        if (info.data_size == base_data_size)
        {
            UInt128 base_checksum = base_backup->getChecksum(path_in_backup);
            if (calculate_checksum() == base_checksum)
            {
                info.base_data_size = info.data_size;
                info.base_checksum = info.checksum;
                infos.emplace(path_in_backup, info);
                return;
            }
        }
        else if (info.data_size > base_data_size)
        {
            UInt128 base_checksum = base_backup->getChecksum(path_in_backup);
            if (calculate_partial_checksum(base_data_size) == base_checksum)
            {
                info.base_data_size = base_data_size;
                info.base_checksum = base_checksum;
                seek(base_data_size);
            }
        }
    }

    UInt64 data_size = entry->getDataSize();
    std::optional<UInt128> checksum = entry->tryGetChecksumFast();
    auto read_buffer = entry->getReadBuffer();
    ReadBuffer * in = read_buffer.get();
    std::optional<HashingReadBuffer> hashing_in;
    if (!checksum.has_value())
        in  = &hashing_in.emplace(*in);

    String out_file_path = directory + path_in_backup;
    disk->createDirectories(directoryPath(out_file_path));
    try
    {
        auto out = disk->writeFile(out_file_path);
        copyData(*in, *out);
        if (out->count() != entry->getDataSize())
        {
            throw Exception(
                std::to_string(out->count()) + " bytes were written to " + quoteString(out_file_path) + " is not equal to the expected number "
                    + std::to_string(entry->getDataSize()),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    catch(...)
    {
        disk->removeFile(out_file_path);
        throw;
    }

    if (!checksum.has_value())
    {
        auto u128 = hashing_in->getHash();
        checksum = UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
    }

    EntryInfo info;
    info.data_size = data_size;
    info.checksum = *checksum;
    infos.emplace(path_in_backup, info);
}

void BackupOnDisk::finishWriting()
{
    writeContents();
    removeLockFile();
    writing_finished = true;
}

}
