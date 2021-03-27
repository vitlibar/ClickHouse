#include <Backup/BackupSnapshotFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <city.h>


namespace DB
{
namespace
{
    UInt128 calculateChecksum(const String & data)
    {
        auto u128 = CityHash_v1_0_2::CityHash128WithSeed(data.data(), data.size(), {0, 0});
        return UInt128{u128.first, u128.second};
    }
}


BackupSnapshotFromMemory::BackupSnapshotFromMemory(const String & name_, const void * data_, size_t size_, const BackupSnapshotParams & params_)
    : BackupSnapshotFromMemory(name_, String{reinterpret_cast<const char *>(data_), size_}, params_)
{
}

BackupSnapshotFromMemory::BackupSnapshotFromMemory(const String & name_, String data_, const BackupSnapshotParams & params_)
    : name(name_), data(std::move(data_)), calculate_checksum(params_.calculate_checksums)
{
}

bool BackupSnapshotFromMemory::getNextEntry(BackupEntry & entry)
{
    if (backup_entry_generated)
        return false;

    entry = {};
    entry.name = name;
    entry.read_buffer = std::make_unique<ReadBufferFromString>(data);
    entry.data_size = data.size();

    if (calculate_checksum)
        entry.checksum = calculateChecksum(data);

    backup_entry_generated = true;
    return true;
}

}
