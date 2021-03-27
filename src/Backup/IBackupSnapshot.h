#pragma once

#include <Core/Types.h>

namespace DB
{
struct BackupEntry;

/// Fixed state of table data prepared to write to a backup.
/// The snapshot keeps references to table data in the exact state
/// as it was when the snapshot was created, which is useful for consistency of the backup
/// because the table can be changed while we're writing the backup.
/// So BackupSnapshot works like a full copy of table data but it's lightweight.
class IBackupSnapshot
{
public:
    virtual ~IBackupSnapshot() = default;
    virtual bool getNextEntry(BackupEntry & entry) = 0;
};


/// Common parameters for making snapshots.
struct BackupSnapshotParams
{
    /// Directory on the source disk where temporary files will be put in.
    String directory_for_temp_files;

    /// Always calculate checksums (this is required for incremental backups).
    bool calculate_checksums = false;
};

}
