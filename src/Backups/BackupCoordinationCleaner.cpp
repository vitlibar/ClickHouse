#include <Backups/BackupCoordinationCleaner.h>


namespace DB
{

BackupCoordinationCleaner::BackupCoordinationCleaner(const String & zookeeper_path_, const WithRetries & with_retries_, LoggerPtr log_)
    : zookeeper_path(zookeeper_path_), with_retries(with_retries_), log(log_)
{
}

void BackupCoordinationCleaner::cleanup()
{
    tryRemoveAllNodes(/* retries_params = */ {}, /* throw_if_error = */ true);
}

bool BackupCoordinationCleaner::tryCleanup() noexcept
{
    return tryRemoveAllNodes(/* retries_params = */ {.error_handling = true}, /* throw_if_error = */ false);
}

bool BackupCoordinationCleaner::tryRemoveAllNodes(const WithRetries::Params & retries_params, bool throw_if_error)
{
    {
        std::lock_guard lock{mutex};
        if (succeeded)
            return true;
        if (failed)
            return false;
    }

    try
    {
        LOG_TRACE(log, "Removing nodes from ZooKeeper");
        auto holder = with_retries.createRetriesControlHolder("removeAllNodes", retries_params);
        holder.retryLoop([&, &zookeeper = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zookeeper);
            zookeeper->removeRecursive(zookeeper_path);
        });

        std::lock_guard lock{mutex};
        succeeded = true;
        return true;
    }
    catch (...)
    {
        std::lock_guard lock{mutex};
        failed = true;

        if (throw_if_error)
            throw;

        LOG_TRACE(log, "Caught exception while removing nodes from ZooKeeper for this restore: {}",
                  getCurrentExceptionMessage(/* with_stacktrace= */ false, /* check_embedded_stacktrace= */ true));
        return false;
    }
}

}
