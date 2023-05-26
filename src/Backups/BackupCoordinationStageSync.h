#pragma once

#include <Backups/WithRetries.h>

namespace DB
{

/// Used to coordinate hosts so all hosts would come to a specific stage at around the same time.
class BackupCoordinationStageSync
{
public:
    BackupCoordinationStageSync(
        const String & backup_zk_path_,
        const Strings & all_hosts_,
        const String & current_host_,
        WithRetries & with_retries_,
        Poco::Logger * log_);

    /// Sets the stage of the current host.
    void setStage(const String & new_stage);

    /// Sets that the BACKUP/RESTORE failed with an error.
    void setError(std::exception_ptr exception);

    /// Waits until all the hosts come to a specified stage. If `timeout` is set the waiting will be limited by the timeout.
    /// If some host failed with an exception this function will rethrow it with information about that host's name.
    void waitForStage(const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout = {});

private:
    void createRootNodes();

#if 0
    struct State;
    State readCurrentState(const Strings & zk_nodes, const Strings & all_hosts, const String & stage_to_wait) const;

    Strings waitImpl(const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const;
#endif

    /// A reference to the field of parent object - BackupCoordinationRemote or RestoreCoordinationRemote
    WithRetries & with_retries;

    const String backup_zk_path;
    const Strings all_hosts;
    const String current_host;
    Poco::Logger * log;
};

}
