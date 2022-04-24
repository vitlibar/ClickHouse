#pragma once

#include <base/types.h>


namespace DB
{

/// Keeps information about files contained in a backup.
class IRestoreCoordination
{
public:
    virtual ~IRestoreCoordination() = default;

    /// Sets that this replica is going to restore a partition in a replicated table or a table in a replicated database.
    /// This function should be called to prevent other replicas from doing that in parallel.
    virtual bool acquirePath(const String & zk_path_, const String & name_) = 0;

    enum Result
    {
        SUCCEEDED,
        FAILED,
    };

    /// Sets the result.
    virtual void setResult(const String & zk_path_, const String & name_, Result res_) = 0;

    /// Wait for the result.
    virtual bool waitForResult(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const = 0;

    /// Removes remotely stored information.
    virtual void drop() {}
};

}
