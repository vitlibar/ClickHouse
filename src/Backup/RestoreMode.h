#pragma once

#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

enum class RestoreMode
{
    /// The destination table must be empty or an exception will be thrown.
    /// This is the safest mode.
    FROM_SCRATCH,

    /// Restore will replace old data. This is a dangerous mode because
    /// the data inserted between making the backup and restoring will be lost.
    REPLACE_OLD_DATA,

    /// Restore will keep both old and new data, which may cause data duplication.
    KEEP_OLD_DATA,
};

inline const char * toString(RestoreMode restore_mode)
{
    switch (restore_mode)
    {
        case RestoreMode::FROM_SCRATCH: return "FROM SCRATCH";
        case RestoreMode::REPLACE_OLD_DATA: return "REPLACE OLD DATA";
        case RestoreMode::KEEP_OLD_DATA: return "KEEP OLD DATA";
    }
    throw Exception("Unknown restore mode: " + std::to_string(static_cast<int>(restore_mode)), ErrorCodes::LOGICAL_ERROR);
}
}
