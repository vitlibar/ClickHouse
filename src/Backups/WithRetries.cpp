#include <Backups/WithRetries.h>

#include <mutex>


namespace DB
{

namespace ErrorCodes
{
    extern const int FAILED_TO_SYNC_BACKUP_OR_RESTORE;
}

WithRetries::WithRetries(
    LoggerPtr log_, zkutil::GetZooKeeper get_zookeeper_, const BackupKeeperSettings & settings_, QueryStatusPtr process_list_element_, bool on_cluster_coordination_, RenewerCallback callback_)
    : log(log_)
    , get_zookeeper(get_zookeeper_)
    , settings(settings_)
    , process_list_element(process_list_element_)
    , on_cluster_coordination(on_cluster_coordination_)
    , callback(callback_)
{}

WithRetries::RetriesControlHolder::RetriesControlHolder(
    const WithRetries * parent, const String & name, const Params & params)
    : info(params.initialization ? parent->settings.max_retries_while_initializing
                              : (params.error_handling ? parent->settings.max_retries_while_handling_error : parent->settings.max_retries),
           parent->settings.retry_initial_backoff_ms.count(),
           parent->settings.retry_max_backoff_ms.count())
    , faulty_zookeeper(parent->getFaultyZooKeeper())
    , retries_ctl(name, parent->log, info, !params.error_handling ? parent->process_list_element : nullptr)
    , on_cluster_coordination(parent->on_cluster_coordination)
{}

WithRetries::RetriesControlHolder WithRetries::createRetriesControlHolder(const String & name, const Params & params) const
{
    return RetriesControlHolder(this, name, params);
}

void WithRetries::RetriesControlHolder::retryLoop(std::function<void()> && f)
{
    try
    {
        retries_ctl.retryLoop(std::move(f));
    }
    catch (const zkutil::KeeperException & e)
    {
        if (!Coordination::isHardwareError(e.code) || !on_cluster_coordination)
            throw;
        throw Exception(getExceptionMessageAndPattern(e, /* with_stacktrace = */ true), ErrorCodes::FAILED_TO_SYNC_BACKUP_OR_RESTORE);
    }
}

void WithRetries::renewZooKeeper(FaultyKeeper my_faulty_zookeeper) const
{
    LOG_INFO(getLogger("!!!"), "WithRetries::renewZooKeeper() - begin");
    std::lock_guard lock(zookeeper_mutex);
    LOG_INFO(getLogger("!!!"), "WithRetries::renewZooKeeper() - locked");

    if (!zookeeper || zookeeper->expired())
    {
        LOG_INFO(getLogger("!!!"), "WithRetries::renewZooKeeper() - getting zk");
        zookeeper = get_zookeeper();
        LOG_INFO(getLogger("!!!"), "WithRetries::renewZooKeeper() - got zk");
        my_faulty_zookeeper->setKeeper(zookeeper);

        if (callback)
            callback(my_faulty_zookeeper);
    }
    else
    {
        my_faulty_zookeeper->setKeeper(zookeeper);
    }
    LOG_INFO(getLogger("!!!"), "WithRetries::renewZooKeeper() - finish");
}

const BackupKeeperSettings & WithRetries::getKeeperSettings() const
{
    return settings;
}

WithRetries::FaultyKeeper WithRetries::getFaultyZooKeeper() const
{
    LOG_INFO(getLogger("!!!"), "WithRetries::getFaultyZooKeeper()");

    zkutil::ZooKeeperPtr current_zookeeper;
    {
        std::lock_guard lock(zookeeper_mutex);
        current_zookeeper = zookeeper;
    }

    LOG_INFO(getLogger("!!!"), "WithRetries::getFaultyZooKeeper() - Middle");

    /// We need to create new instance of ZooKeeperWithFaultInjection each time and copy a pointer to ZooKeeper client there
    /// The reason is that ZooKeeperWithFaultInjection may reset the underlying pointer and there could be a race condition
    /// when the same object is used from multiple threads.
    auto faulty_zookeeper = ZooKeeperWithFaultInjection::createInstance(
        settings.fault_injection_probability,
        settings.fault_injection_seed,
        current_zookeeper,
        log->name(),
        log);

    LOG_INFO(getLogger("!!!"), "WithRetries::getFaultyZooKeeper() - returning");
    return faulty_zookeeper;
}


}
