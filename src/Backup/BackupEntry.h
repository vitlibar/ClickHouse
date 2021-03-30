#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>
#include <optional>

namespace DB
{
class ReadBuffer;

struct BackupEntry
{
    /// Name of the entry, not empty.
    /// Usually it looks like a path and starts with "data/" or "metadata/".
    String name;

    /// Function returning a ReadBuffer to read the entry's data.
    std::function<std::unique_ptr<ReadBuffer>()> get_read_buffer_function;

    /// Size of the data.
    size_t data_size;

    /// Checksum for the data, to be able to skip storing the entry if it's already stored
    /// when incremental backup is enabled.
    std::optional<UInt128> checksum;

    BackupEntry();
    ~BackupEntry();
    BackupEntry(BackupEntry && src);
    BackupEntry & operator =(BackupEntry && src);
};

}
