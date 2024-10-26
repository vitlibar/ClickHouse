#include <Backups/BackupCoordinationOnCluster.h>

#include <Backups/BackupCoordinationStage.h>
#include <Backups/BackupCoordinationStageSync.h>
#include <Backups/RestoreCoordinationOnCluster.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CreateQueryUUIDs.h>
#include <Parsers/formatAST.h>
#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <Common/escapeForFileName.h>


namespace DB
{

RestoreCoordinationOnCluster::RestoreCoordinationOnCluster(
    const UUID & restore_uuid_,
    const String & root_zookeeper_path_,
    zkutil::GetZooKeeper get_zookeeper_,
    const BackupKeeperSettings & keeper_settings_,
    const String & current_host_,
    const Strings & all_hosts_,
    bool allow_concurrent_restore_,
    BackupConcurrencyCounters & concurrency_counters_,
    ThreadPoolCallbackRunnerUnsafe<void> schedule_,
    QueryStatusPtr process_list_element_)
    : root_zookeeper_path(root_zookeeper_path_)
    , keeper_settings(keeper_settings_)
    , restore_uuid(restore_uuid_)
    , zookeeper_path(root_zookeeper_path_ + "/restore-" + toString(restore_uuid_))
    , all_hosts(all_hosts_)
    , current_host(current_host_)
    , current_host_index(BackupCoordinationOnCluster::findCurrentHostIndex(current_host, all_hosts))
    , log(getLogger("RestoreCoordinationOnCluster"))
    , with_retries(log, get_zookeeper_, keeper_settings, process_list_element_, /* on_cluster_coordination = */ true, [root_zookeeper_path_](Coordination::ZooKeeperWithFaultInjection::Ptr zk) { zk->sync(root_zookeeper_path_); })
    , concurrency_check(/* is_restore = */ true, restore_uuid_, /* on_cluster = */ true, allow_concurrent_restore_, concurrency_counters_)
    , stage_sync(/* is_restore = */ true, fs::path{zookeeper_path} / "stage", current_host, allow_concurrent_restore_, with_retries, schedule_, process_list_element_, log)
    , cleaner(zookeeper_path, with_retries, log)
{
    chassert(std::find(all_hosts.begin(), all_hosts.end(), kInitiator) == all_hosts.end());
    createRootNodes();
}

RestoreCoordinationOnCluster::~RestoreCoordinationOnCluster()
{
    /// tryCleanupImpl() must not throw any exceptions.
    //tryCleanupImpl();
}

void RestoreCoordinationOnCluster::createRootNodes()
{
    auto holder = with_retries.createRetriesControlHolder("createRootNodes", {.initialization = true});
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            zk->createAncestors(zookeeper_path);
            zk->createIfNotExists(zookeeper_path, "");
            zk->createIfNotExists(zookeeper_path + "/repl_databases_tables_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_tables_data_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_access_storages_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/repl_sql_objects_acquired", "");
            zk->createIfNotExists(zookeeper_path + "/keeper_map_tables", "");
            zk->createIfNotExists(zookeeper_path + "/table_uuids", "");
        });
}

void RestoreCoordinationOnCluster::setStage(const String & new_stage, const String & message)
{
    stage_sync.setStage(new_stage, message);
}

void RestoreCoordinationOnCluster::setError(const Exception & exception)
{
    stage_sync.setError(exception);
}

Strings RestoreCoordinationOnCluster::waitForStage(const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout)
{
    return stage_sync.waitForHostsToReachStage(stage_to_wait, all_hosts, timeout);
}

std::chrono::seconds RestoreCoordinationOnCluster::getOnClusterInitializationTimeout() const
{
    return keeper_settings.on_cluster_initialization_timeout;
}

bool RestoreCoordinationOnCluster::acquireCreatingTableInReplicatedDatabase(const String & database_zk_path, const String & table_name)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireCreatingTableInReplicatedDatabase");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_databases_tables_acquired/" + escapeForFileName(database_zk_path);
            zk->createIfNotExists(path, "");

            path += "/" + escapeForFileName(table_name);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationOnCluster::acquireInsertingDataIntoReplicatedTable(const String & table_zk_path)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireInsertingDataIntoReplicatedTable");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_tables_data_acquired/" + escapeForFileName(table_zk_path);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationOnCluster::acquireReplicatedAccessStorage(const String & access_storage_zk_path)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireReplicatedAccessStorage");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_access_storages_acquired/" + escapeForFileName(access_storage_zk_path);
            auto code = zk->tryCreate(path, toString(current_host_index), zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationOnCluster::acquireReplicatedSQLObjects(const String & loader_zk_path, UserDefinedSQLObjectType object_type)
{
    bool result = false;
    auto holder = with_retries.createRetriesControlHolder("acquireReplicatedSQLObjects");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/repl_sql_objects_acquired/" + escapeForFileName(loader_zk_path);
            zk->createIfNotExists(path, "");

            path += "/";
            switch (object_type)
            {
                case UserDefinedSQLObjectType::Function:
                    path += "functions";
                    break;
            }

            auto code = zk->tryCreate(path, "", zkutil::CreateMode::Persistent);
            if ((code != Coordination::Error::ZOK) && (code != Coordination::Error::ZNODEEXISTS))
                throw zkutil::KeeperException::fromPath(code, path);

            if (code == Coordination::Error::ZOK)
            {
                result = true;
                return;
            }

            /// We need to check who created that node
            result = zk->get(path) == toString(current_host_index);
        });
    return result;
}

bool RestoreCoordinationOnCluster::acquireInsertingDataForKeeperMap(const String & root_zk_path, const String & table_unique_id)
{
    bool lock_acquired = false;
    auto holder = with_retries.createRetriesControlHolder("acquireInsertingDataForKeeperMap");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            /// we need to remove leading '/' from root_zk_path
            auto normalized_root_zk_path = root_zk_path.substr(1);
            std::string restore_lock_path = fs::path(zookeeper_path) / "keeper_map_tables" / escapeForFileName(normalized_root_zk_path);
            zk->createAncestors(restore_lock_path);
            auto code = zk->tryCreate(restore_lock_path, table_unique_id, zkutil::CreateMode::Persistent);

            if (code == Coordination::Error::ZOK)
            {
                lock_acquired = true;
                return;
            }

            if (code == Coordination::Error::ZNODEEXISTS)
                lock_acquired = table_unique_id == zk->get(restore_lock_path);
            else
                zkutil::KeeperException::fromPath(code, restore_lock_path);
        });
    return lock_acquired;
}

void RestoreCoordinationOnCluster::generateUUIDForTable(ASTCreateQuery & create_query)
{
    String query_str = serializeAST(create_query);
    CreateQueryUUIDs new_uuids{create_query, /* generate_random= */ true, /* force_random= */ true};
    String new_uuids_str = new_uuids.toString();

    auto holder = with_retries.createRetriesControlHolder("generateUUIDForTable");
    holder.retryLoop(
        [&, &zk = holder.faulty_zookeeper]()
        {
            with_retries.renewZooKeeper(zk);

            String path = zookeeper_path + "/table_uuids/" + escapeForFileName(query_str);
            Coordination::Error res = zk->tryCreate(path, new_uuids_str, zkutil::CreateMode::Persistent);

            if (res == Coordination::Error::ZOK)
            {
                new_uuids.copyToQuery(create_query);
                return;
            }

            if (res == Coordination::Error::ZNODEEXISTS)
            {
                CreateQueryUUIDs::fromString(zk->get(path)).copyToQuery(create_query);
                return;
            }

            zkutil::KeeperException::fromPath(res, path);
        });
}

void RestoreCoordinationOnCluster::cleanup()
{
    if (current_host == kInitiator)
        stage_sync.waitForHostsToFinish(all_hosts);

    bool all_hosts_finished = false;
    stage_sync.finish(all_hosts_finished);

    if (all_hosts_finished)
        cleaner.cleanup();
}

bool RestoreCoordinationOnCluster::tryCleanup() noexcept
{
    return tryCleanupImpl();
}

bool RestoreCoordinationOnCluster::tryCleanupImpl() noexcept
{
    if (current_host == kInitiator)
        stage_sync.tryWaitForHostsToFinish(all_hosts);

    bool all_hosts_finished;
    bool ok = stage_sync.tryFinish(all_hosts_finished);
    if (ok && all_hosts_finished)
        ok = cleaner.tryCleanup();

    return ok;
}

}
