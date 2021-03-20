#pragma once

#include <Core/Types.h>
#include <Common/UInt128.h>
#include <optional>

namespace DB
{
class ReadBuffer;

struct BackupEntry
{
    /// Relative path to the file, usually it starts with either "data" or "metadata".
    /// Never empty (even for the table engine Memory).
    String path;

    /// ReadBuffer to read the file's contents. Can be nullptr.
    std::unique_ptr<ReadBuffer> read_buffer;

    /// The file's contents, used only if `read_buffer` is nullptr.
    String contents;

    /// Checksum for the contents of the file, to be able to skip the backup entry if it's already stored
    /// when incremental backup is enabled. If not set it will be calculated from the contents of the file.
    std::optional<UInt128> checksum;

    /// Whether this file is a symlink or a hard link. The target's path is stored in `contents`.
    bool is_symlink = false;
    bool is_hard_link = false;
};

}
