#include <Backups/BackupCoordinationConcurrencyFinder.h>


namespace DB
{

namespace
{
    
}

void checkNoConcurrentBackups(
    const String & backup_id,
    const UUID & backup_uuid,
    std::unordered_map<UUID, String> & active_backups_ids,
    std::mutex & active_backups_mutex)
{
    std::lock_guard lock{active_backups_mutex};
    for (const auto & [uuid, id] : active_backups_ids)
        if (uuid != backup_uuid)
            throw Exception(ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE, "Found concurrent local backup with id={}", id);
    active_backups_ids[backup_uuid] = backup_id;
}


void checkNoConcurrentBackups(
    const String & backup_id,
    const UUID & backup_uuid,
    std::vector<std::pair<UUID, String>> & active_backups_ids,
    std::mutex & active_backups_mutex,
    const String & current_host,
    zkutil::GetZooKeeper get_zookeeper,
    const String & root_zookeeper_path,
    const BackupKeeperSettings & keeper_settings)
{

}









BackupConcurrentBackupsFinderRemote::BackupConcurrentBackupsFinderRemote(
    const String & root_zookeeper_path_,
    WithRetries & with_retries_,
    Poco::Logger * log_,
    const UUID & backup_or_restore_uuid_)
    : root_zookeeper_path(root_zookeeper_path_)
    , with_retries(with_retries_)
    , log(log_)
    , backup_or_restore_uuid(backup_or_restore_uuid_)
    , timeout_if_no_alive_nodes_ms(timeout_if_no_alive_nodes_ms_)
{
}

std::optional<String> BackupConcurrentBackupsFinderRemote::getAnyConcurrentBackup() const
{
    if (auto local_res = local_finder.getAnyConcurrentBackup())
        return local_res;
    return getAnyConcurrentImpl("backup");
}

std::optional<String> BackupConcurrentBackupsFinderRemote::getAnyConcurrentRestore() const
{
    if (auto local_res = local_finder.getAnyConcurrentRestore())
        return local_res;
    return getAnyConcurrentImpl("restore");
}

namespace
{
    struct OperationsWithoutAliveNode
    {
        struct Info
        {
            String id;
            size_t num_statuses = 0;
        };
        std::unordered_map<String, Info> infos;
        std::chrono::steady_clock::time_point check_time;
    };
}

std::optional<String> BackupConcurrentBackupsFinderRemote::getAnyConcurrentImpl(const String & type) const
{
    std::optional<String> found_id;
    std::optional<OperationsWithoutAliveNode> operations_without_alive_node;

    auto holder = with_retries.createRetriesControlHolder("getAnyConcurrent" + type);
    holder.retries_ctl.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
    {
        with_retries.renewZooKeeper(zk);

        if (!zk->exists(root_zookeeper_path))
            return;

        Strings operation_paths;
        auto code = zk->tryGetChildren(root_zookeeper_path, operation_paths);
        if (code == Coordination::Error::ZNONODE)
            return;
        if (code != Coordination::Error::ZOK)
            throw zkutil::KeeperException::fromPath(code, root_zookeeper_path);

        if (operation_paths.empty())
            return;

        String prefix = type + "-";
        OperationsWithoutAliveNode new_operations_without_alive_node;

        for (const auto & operation_path : operation_paths)
        {
            if (!startsWith(operation_path, prefix))
                continue;

            String existing_uuid = operation_path.substr(prefix.length());
            if (existing_uuid == toString(backup_or_restore_uuid))
                continue;

            String id;
            String path = root_zookeeper_path + "/" + operation_path + "/id";
            code = zk->tryGet(path, id);
            if (code == Coordination::Error::ZNONODE)
                continue;
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(code, path);

            Strings statuses;
            String path = root_zookeeper_path + "/" + existing_path + "/stage";
            code = zk->tryGetChildren(path, statuses);
            if (code == Coordination::Error::ZNONODE)
                continue;
            if (code != Coordination::Error::ZOK)
                throw zkutil::KeeperException::fromPath(code, path);

            for (const auto & status : statuses)
            {
                if (status.startsWith("alive|"))
                {
                    /// "alive" node exists
                    found_id = id;
                    return;
                }
            }

            if (operations_without_alive_node)
            {
                auto it = operations_without_alive_node->infos.find(operation_path);
                if ((it == operations_without_alive_node->infos.end()) || (it->second.num_statuses < statuses.size()))
                {
                    found_id = id;
                    return;
                }
            }

            if (!new_operations_without_alive_node)
            {
                new_operations_without_alive_node.emplace();
                new_operations_without_alive_node->check_time = std::chrono::steady_clock::time_point::now();
            }
            new_operations_without_alive_node->infos[operation_path] = Info{.id = id, num_statuses = statuses.size()};
        }

        if (operations_without_alive_node)
        {
            std::erase_if(
                operations_without_alive_node.id_by_path,
                [](const String & operation_path) { return !new_operations_without_alive_node->id_by_path.contains(operation_path); });
        }
        else
        {
            operations_without_alive_node = std::move(new_operations_without_alive_node);
        }

        if (operations_without_alive_nodes->id_by_path.empty())
            return;

        if (timeout_if_no_alive_nodes_ms
            && (std::chrono::steady_clock::time_point::now() - operations_without_alive_node->check_time)
                > std::chrono::milliseconds{timeout_if_no_alive_nodes_ms})
            return;

        holder.setUserError(
            ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE,
            "Not found 'alive' nodes for {} with id=\"{}\" (zookeeper path={}){}",
            type,
            operations_without_alive_node->id_by_path.begin()->second,
            operations_without_alive_node->id_by_path.begin()->first,
            (!holder.retries_ctl.isLastRetry() ? ", will retry" : ""));
    });

    return found_id;
}


BackupConcurrentBackupsFinderLocal::BackupConcurrentBackupsFinderLocal(const UUID & backup_or_restore_uuid_)
    : backup_or_restore_uuid(backup_or_restore_uuid_)
{
}

std::optional<String> BackupConcurrentBackupsFinderLocal::getAnyConcurrentBackup() const
{
    auto ids = backups_worker.getActiveBackupsUUIDsAndIDs(/* max_count= */ 2);
    if (ids.empty())
        return {};
    if (ids[0].first != backup_or_restore_uuid)
        return ids[0].second;
    if (ids.size() == 1)
        return {};
    return ids[1].second;
}

std::optional<String> BackupConcurrentBackupsFinderLocal::getAnyConcurrentBackup() const
{
    auto ids = backups_worker.getActiveRestoresUUIDsAndIDs(/* max_count= */ 2);
    if (ids.empty())
        return {};
    if (ids[0].first != backup_or_restore_uuid)
        return ids[0].second;
    if (ids.size() == 1)
        return {};
    return ids[1].second;
}

}
