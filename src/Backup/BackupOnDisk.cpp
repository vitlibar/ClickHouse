#include <Backup/BackupOnDisk.h>
#include <Backup/BackupEntry.h>
#include <Common/quoteString.h>
#include <Disks/DiskSelector.h>
#include <Disks/IDisk.h>
#include <IO/HashingReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
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
    extern const UInt64 BACKUP_VERSION = 1;

    String entryNameToPathInBackup(const String & name)
    {
        String res;
        res.reserve(name.length());
        for (size_t i : ext::range(name.length()))
        {
            char c = name[i];
            if (isWordCharASCII(c))
                res += c;
            else if (c == '/' && (i >= 1 && isWordCharASCII(name[i - 1])))
                res += c;
            else
            {
                res += '%';
                res += hexDigitUppercase(c / 16);
                res += hexDigitUppercase(c % 16);
            }
        }
        return res;
    }
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
    }

    if (open_mode == OpenMode::READ)
        readHeader();
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

void BackupOnDisk::writeHeader()
{
    auto out = disk->writeFile(directory + ".header");
    writeVarUInt(BACKUP_VERSION, *out);

    writeVarUInt(entries.size(), *out);
    for (const auto & [name, entry] : entries)
    {
        writeBinary(name, *out);
        writeVarUInt(entry.data_size, *out);
        writeBinary(entry.checksum, *out);
        writeVarUInt(entry.read_from_base, *out);
    }

    if (base_backup)
    {
        writeBinary(base_backup->getDiskName(), *out);
        writeBinary(base_backup->getPath(), *out);
    }
    else
    {
        writeString("", *out);
        writeString("", *out);
    }
}

void BackupOnDisk::readHeader()
{
    auto in = disk->readFile(directory + ".header");
    UInt64 version;
    readVarUInt(version, *in);
    if (version != BACKUP_VERSION)
        throw Exception("Backup version " + std::to_string(version) + " is not supported",
                        ErrorCodes::BACKUP_VERSION_NOT_SUPPORTED);

    size_t num_entries;
    readVarUInt(num_entries, *in);
    entries.clear();
    for (size_t i : ext::range(num_entries))
    {
        String name;
        readBinary(name, *in);
        Entry entry;
        readVarUInt(entry.data_size, *in);
        readBinary(entry.checksum, *in);
        readVarUInt(entry.read_from_base, *in);
        entries.emplace(name, entry);
    }

    String base_backup_disk_name, base_backup_path;
    readBinary(base_backup_disk_name, *in);
    readBinary(base_backup_path, *in);
    base_backup = nullptr;
    if (!base_backup_disk_name.empty())
        base_backup = std::make_shared<BackupOnDisk>(OpenMode::READ, base_backup_disk_name, base_backup_path, disk_selector);
}

IBackup::OpenMode BackupOnDisk::getOpenMode() const
{
    return open_mode;
}

String BackupOnDisk::getDiskName() const
{
    return disk->getName();
}

String BackupOnDisk::getPath() const
{
    return directory;
}

Strings BackupOnDisk::list() const
{
    std::lock_guard lock{mutex};
    Strings names;
    for (const String & name : entries | boost::adaptors::map_keys)
        names.push_back(name);
    return names;
}

bool BackupOnDisk::exists(const String & name) const
{
    std::lock_guard lock{mutex};
    return entries.count(name) != 0;
}

size_t BackupOnDisk::getDataSize(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(name);
    if (it == entries.end())
        throw Exception("Entry " + quoteString(name) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);
    return it->second.data_size;
}

UInt128 BackupOnDisk::getChecksum(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(name);
    if (it == entries.end())
        throw Exception("Entry " + quoteString(name) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);
    return it->second.checksum;
}

BackupEntry BackupOnDisk::read(const String & name) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(name);
    if (it == entries.end())
        throw Exception("Entry " + quoteString(name) + " not found in the backup", ErrorCodes::BACKUP_ENTRY_NOT_EXISTS);

    BackupEntry entry;
    if (it->second.read_from_base)
    {
        if (!base_backup)
        {
            throw Exception(
                "Entry is marked as to be read from a base backup, but the base backup is not available", ErrorCodes::BACKUP_DAMAGED);
        }
        entry = base_backup->read(name);
        if ((entry.data_size != it->second.data_size) || (*entry.checksum != it->second.checksum))
        {
            throw Exception("Entry from a base backup has a different size or a checksum", ErrorCodes::BACKUP_DAMAGED);
        }
    }
    else
    {
        entry.name = name;
        entry.data_size = it->second.data_size;
        entry.checksum = it->second.checksum;
        String file_path = directory + entryNameToPathInBackup(entry.name);
        entry.get_read_buffer_function = [this, file_path]() -> std::unique_ptr<ReadBuffer> { return disk->readFile(file_path); };
    }
    return entry;
}

void BackupOnDisk::write(BackupEntry && entry)
{
    std::lock_guard lock{mutex};
    auto it = entries.find(entry.name);
    if (it != entries.end())
        throw Exception("Entry " + quoteString(entry.name) + " already exists in the backup", ErrorCodes::BACKUP_ENTRY_EXISTS);

    if (base_backup && base_backup->exists(entry.name))
    {
        size_t data_size_in_base_backup = base_backup->getDataSize(entry.name);
        if (entry.data_size == data_size_in_base_backup)
        {
            UInt128 checksum_in_base_backup = base_backup->getChecksum(entry.name);
            if (!entry.checksum)
            {
                auto read_buffer = entry.get_read_buffer_function();
                HashingReadBuffer hashing_in{*read_buffer};
                hashing_in.ignoreAll();
                auto u128 = hashing_in.getHash();
                entry.checksum = UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
            }
            if (*entry.checksum == checksum_in_base_backup)
            {
                Entry new_entry;
                new_entry.data_size = data_size_in_base_backup;
                new_entry.checksum = checksum_in_base_backup;
                new_entry.read_from_base = true;
                entries.emplace(entry.name, new_entry);
            }
        }
    }

    {
        auto read_buffer = entry.get_read_buffer_function();
        ReadBuffer * in = read_buffer.get();
        std::optional<HashingReadBuffer> hashing_in;
        if (!entry.checksum)
            in  = &hashing_in.emplace(*in);

        String file_path = directory + entryNameToPathInBackup(entry.name);
        disk->createDirectories(directoryPath(file_path));
        try
        {
            auto out = disk->writeFile(file_path);
            copyData(*in, *out);
        }
        catch(...)
        {
            disk->removeFile(file_path);
            throw;
        }

        if (!entry.checksum)
        {
            auto u128 = hashing_in->getHash();
            entry.checksum = UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
        }
    }

    Entry new_entry;
    new_entry.data_size = entry.data_size;
    new_entry.checksum = *entry.checksum;
    new_entry.read_from_base = false;
    entries.emplace(entry.name, new_entry);
}

void BackupOnDisk::finishWriting()
{
    writeHeader();
    removeLockFile();
    writing_finished = true;
}

}
