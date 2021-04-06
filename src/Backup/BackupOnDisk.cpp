#include <Backup/BackupOnDisk.h>
#include <Backup/BackupEntryFromFile.h>
#include <Backup/IBackupEntry.h>
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

    String escapePathInBackup(const String & name)
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

    writeVarUInt(infos.size(), *out);
    for (const auto & [path_in_backup, info] : infos)
    {
        writeBinary(path_in_backup, *out);
        writeVarUInt(info.data_size, *out);
        writeBinary(info.checksum, *out);
        writeVarUInt(info.from_base, *out);
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

    size_t num_infos;
    readVarUInt(num_infos, *in);
    infos.clear();
    for (size_t i : ext::range(num_infos))
    {
        String path_in_backup;
        readBinary(path_in_backup, *in);
        EntryInfo new_info;
        readVarUInt(new_info.data_size, *in);
        readBinary(new_info.checksum, *in);
        readVarUInt(new_info.from_base, *in);
        infos.emplace(path_in_backup, new_info);
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
    for (const String & name : infos | boost::adaptors::map_keys)
        names.push_back(name);
    return names;
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

    if (it->second.from_base)
    {
        if (!base_backup)
        {
            throw Exception(
                "Entry is marked as to be read from a base backup, but the base backup is not available", ErrorCodes::BACKUP_DAMAGED);
        }
        auto entry_from_base = base_backup->read(path_in_backup);
        if ((entry_from_base->getDataSize() != it->second.data_size) || (entry_from_base->getChecksum() != it->second.checksum))
        {
            throw Exception("Entry from a base backup has a different size or a checksum", ErrorCodes::BACKUP_DAMAGED);
        }
        return entry_from_base;
    }

    return std::make_unique<BackupEntryFromFile>(
        name, disk, directory + escapePathInBackup(path_in_backup), it->second.data_size, it->second.checksum);
}

void BackupOnDisk::write(std::unique_ptr<IBackupEntry> entry)
{
    std::lock_guard lock{mutex};
    const String & path_in_backup = entry->getPathInBackup();
    auto it = infos.find(path_in_backup);
    if (it != infos.end())
        throw Exception("Entry " + quoteString(path_in_backup) + " already exists in the backup", ErrorCodes::BACKUP_ENTRY_EXISTS);

    if (base_backup && base_backup->exists(path_in_backup))
    {
        size_t data_size_from_base = base_backup->getDataSize(path_in_backup);
        if (entry->getDataSize() == data_size_from_base)
        {
            UInt128 checksum_from_base = base_backup->getChecksum(path_in_backup);
            if (entry->getChecksum() == checksum_from_base)
            {
                EntryInfo new_info;
                new_info.data_size = data_size_from_base;
                new_info.checksum = checksum_from_base;
                new_info.from_base = true;
                infos.emplace(path_in_backup, new_info);
                return;
            }
        }
    }

    std::optional<UInt64> data_size = entry->tryGetDataSize();
    std::optional<UInt128> checksum = entry->tryGetChecksum();
    auto read_buffer = entry->getReadBuffer();
    ReadBuffer * in = read_buffer.get();
    std::optional<HashingReadBuffer> hashing_in;
    if (!checksum.has_value())
        in  = &hashing_in.emplace(*in);

    String out_file_path = directory + escapePathInBackup(path_in_backup);
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

    if (!data_size.has_value())
        data_size = in->count();

    if (!checksum.has_value())
    {
        auto u128 = hashing_in->getHash();
        checksum = UInt128{CityHash_v1_0_2::Uint128Low64(u128), CityHash_v1_0_2::Uint128High64(u128)};
    }

    EntryInfo new_info;
    new_info.data_size = *data_size;
    new_info.checksum = *checksum;
    new_info.from_base = false;
    infos.emplace(path_in_backup, new_info);
}

void BackupOnDisk::finishWriting()
{
    writeHeader();
    removeLockFile();
    writing_finished = true;
}

}
