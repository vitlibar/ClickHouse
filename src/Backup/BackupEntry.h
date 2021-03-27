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

    /// ReadBuffer to read the entry's data, not null.
    std::unique_ptr<ReadBuffer> read_buffer;

    /// Size of the data.
    size_t data_size;

    /// Checksum for the data, to be able to skip storing the entry if it's already stored
    /// when incremental backup is enabled.
    std::optional<UInt128> checksum;
};

}
