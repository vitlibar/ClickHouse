#include <Backup/BackupSnapshotFromMemory.h>
#include <IO/ReadBufferFromString.h>


namespace DB
{

BackupSnapshotFromMemory::BackupSnapshotFromMemory(const String & name_, const void * data_, size_t size_, const BackupSnapshotParams & params_)
    : BackupSnapshotFromMemory(name_, String{reinterpret_cast<const char *>(data_), size_}, params_)
{
}

BackupSnapshotFromMemory::BackupSnapshotFromMemory(const String & name_, String data_, const BackupSnapshotParams &)
    : name(name_), data(std::move(data_))
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

    backup_entry_generated = true;
    return true;
}

}
