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
    const UInt64 BACKUP_CONTENTS_VERSION = 1;
    const UInt128 CHECKSUM_OF_EMPTY_DATA{0, 0};
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
        writeVarUInt(info.size, *out);
        if (info.size)
        {
            writeBinary(info.checksum, *out);
            writeVarUInt(info.base_size, *out);
            if (info.base_size && (info.base_size != info.size))
                writeBinary(info.base_checksum, *out);
        }
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
        readVarUInt(info.size, *in);
        if (info.size)
        {
            readBinary(info.checksum, *in);
            readVarUInt(info.base_size, *in);
            if (info.base_size && (info.base_size != info.size))
                readBinary(info.base_checksum, *in);
            else if (info.base_size)
                info.base_checksum = info.checksum;
        }
        else
        {
            info.checksum = CHECKSUM_OF_EMPTY_DATA;
        }
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

size_t BackupOnDisk::getSize(const String & path_in_backup) const
{
    std::lock_guard lock{mutex};
    auto it = infos.find(path_in_backup);
    if (it == infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);
    return it->second.size;
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
    if (!info.size)
    {
        /// Entry's data is empty.
        return std::make_unique<BackupEntryFromMemory>(path_in_backup, nullptr, 0, CHECKSUM_OF_EMPTY_DATA);
    }

    if (!info.base_size)
    {
        /// Data goes completely from this backup, the base backup isn't used.
        return std::make_unique<BackupEntryFromFile>(
            path_in_backup, disk, directory + path_in_backup, BackupEntryFromFile::ALWAYS_EXISTS_IMMUTABLE, info.size, info.checksum);
    }

    if (!base_backup)
    {
        throw Exception(
            "Entry is marked as to be read from a base backup, but the base backup is not available", ErrorCodes::BACKUP_DAMAGED);
    }

    auto base_entry = base_backup->read(path_in_backup);
    auto base_size = base_entry->getSize();
    if (base_size != info.base_size)
    {
        throw Exception(
            "Entry " + quoteString(path_in_backup) + " has unexpected size in the base backup (" + toString(base_size)
                + " != " + toString(info.base_size) + ")",
            ErrorCodes::BACKUP_DAMAGED);
    }

    auto base_checksum = base_entry->getChecksum();
    if (base_checksum && (*base_checksum != info.base_checksum))
    {
        throw Exception("Entry " + quoteString(path_in_backup) + " has unexpected checksum in the base backup", ErrorCodes::BACKUP_DAMAGED);
    }

    if (info.size == info.base_size)
    {
        /// Data goes completely from the base backup (nothing goes from this backup).
        return base_entry;
    }

    if (info.size < info.base_size)
    {
        throw Exception(
            "Entry " + quoteString(path_in_backup) + " has its data size less than the one in the base backup (" + toString(info.size)
                + " < " + toString(info.base_size) + ")",
            ErrorCodes::BACKUP_DAMAGED);
    }

    /// The beginning of the data goes from the base backup,
    /// and the ending goes from this backup.
    return std::make_unique<BackupEntryFromConcat>(
        path_in_backup,
        std::move(base_entry),
        std::make_unique<BackupEntryFromFile>(
            path_in_backup,
            disk,
            directory + path_in_backup,
            BackupEntryFromFile::ALWAYS_EXISTS_IMMUTABLE,
            info.size - info.base_size),
        info.checksum);
}


void BackupOnDisk::write(std::unique_ptr<IBackupEntry> entry)
{
    std::unique_ptr<ReadBuffer> read_buffer;
    std::optional<HashingReadBuffer> hashing_buffer;

    /// Lazily gets a read buffer for reading the entry's data.
    auto get_read_buffer = [&]() -> ReadBuffer *
    {
        if (!read_buffer)
            read_buffer = entry->getReadBuffer();
        return read_buffer.get();
    };

    /// Restarts reading the entry's data from the beginning.
    auto seek_set = [&]()
    {
        hashing_buffer.reset();
        auto * seekable_buffer = typeid_cast<SeekableReadBuffer *>(get_read_buffer());
        if (seekable_buffer)
            seekable_buffer->seek(0, SEEK_SET);
        else if (read_buffer->count())
            read_buffer = entry->getReadBuffer();
    };

    std::optional<UInt128> calculated_checksum = entry->getChecksum();

    /// Calculates the checksum for the entry's data if it wasn't calculated before.
    auto calculate_checksum = [&]() -> UInt128
    {
        if (!calculated_checksum)
        {
            hashing_buffer.emplace(*get_read_buffer());
            hashing_buffer->ignoreAll();
            auto u128 = hashing_buffer->getHash();
            get_read_buffer()->position() = hashing_buffer->position();
            calculated_checksum = UInt128{u128.first, u128.second};
        }
        return *calculated_checksum;
    };

    /// Calculates the checksum for some part of the entry's data.
    auto calculate_partial_checksum = [&](UInt64 limit) -> UInt128
    {
        if (limit >= entry->getSize())
            return calculate_checksum();
        hashing_buffer.emplace(*get_read_buffer());
        hashing_buffer->ignore(limit);
        auto u128 = hashing_buffer->getHash();
        get_read_buffer()->position() = hashing_buffer->position();
        return UInt128{u128.first, u128.second};
    };

    std::lock_guard lock{mutex};
    const String & path_in_backup = entry->getPathInBackup();
    auto it = infos.find(path_in_backup);
    if (it != infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " already exists in the backup", ErrorCodes::BACKUP_ENTRY_EXISTS);

    UInt64 size = entry->getSize();
    if (!size)
    {
        /// Entry's data is empty.
        EntryInfo info;
        info.size = 0;
        info.checksum = CHECKSUM_OF_EMPTY_DATA;
        infos.emplace(path_in_backup, info);
        return;
    }

    /// Check if the entry's data can be received from the base backup,
    /// completely or just the beginning.
    UInt64 base_size = 0;
    UInt128 base_checksum{0, 0};
    if (base_backup && base_backup->exists(path_in_backup))
    {
        UInt64 maybe_base_size = base_backup->getSize(path_in_backup);
        if ((size >= maybe_base_size) && maybe_base_size)
        {
            auto maybe_base_checksum = base_backup->getChecksum(path_in_backup);
            if (calculate_partial_checksum(base_size) == maybe_base_checksum)
            {
                /// Checksum matches, so we can store only the data after `base_size`
                /// in this backup. The current position in the read buffer is already
                /// at `base_size` (after calculating the partial checksum),
                /// so we don't have to move it.
                base_size = maybe_base_size;
                base_checksum = maybe_base_checksum;
            }
            else if (hashing_buffer)
            {
                /// Checksum doesn't match, but we moved the current position in
                /// the read buffer while we were calculating the checksum. So we need to
                /// move that position back to the beginning of the read buffer.
                seek_set();
            }
        }
    }

    if (size == base_size)
    {
        /// The entry's data goes completely from the base backup
        /// (nothing goes from this backup).
        EntryInfo info;
        info.size = base_size;
        info.checksum = base_checksum;
        info.base_size = base_size;
        info.base_checksum = base_checksum;
        infos.emplace(path_in_backup, info);
        return;
    }

    /// Initialize a read buffer for copying the entry's data.
    ReadBuffer * input_for_copying = get_read_buffer();
    if (calculated_checksum)
    {
        /// The checksum is already known, we don't need the hashing buffer anymore.
        hashing_buffer.reset();
    }
    else
    {
        /// The checksum wasn't calculated yet, so we have to calculate it now.
        if (!hashing_buffer)
            hashing_buffer.emplace(*input_for_copying);
        input_for_copying = &hashing_buffer.value();
    }

    /// Copy the entry's data after `base_size`.
    String out_file_path = directory + path_in_backup;
    disk->createDirectories(directoryPath(out_file_path));
    try
    {
        auto out = disk->writeFile(out_file_path);
        copyData(*input_for_copying, *out);
        if (out->count() != entry->getSize())
        {
            throw Exception(
                std::to_string(out->count()) + " bytes were written to " + quoteString(out_file_path)
                    + " is not equal to the expected number " + std::to_string(entry->getSize()),
                ErrorCodes::LOGICAL_ERROR);
        }
    }
    catch(...)
    {
        disk->removeFileIfExists(out_file_path);
        throw;
    }

    if (!calculated_checksum)
    {
        auto u128 = hashing_buffer->getHash();
        calculated_checksum = UInt128{u128.first, u128.second};
    }

    /// Done!
    EntryInfo info;
    info.size = size;
    info.checksum = *calculated_checksum;
    info.base_size = base_size;
    info.base_checksum = base_checksum;
    infos.emplace(path_in_backup, info);
}

void BackupOnDisk::finishWriting()
{
    writeContents();
    removeLockFile();
    writing_finished = true;
}

}
