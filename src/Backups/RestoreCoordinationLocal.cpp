#include <Backups/RestoreCoordinationLocal.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


RestoreCoordinationLocal::RestoreCoordinationLocal() = default;
RestoreCoordinationLocal::~RestoreCoordinationLocal() = default;

bool RestoreCoordinationLocal::acquirePath(const String & path_, const String & name_)
{
    std::lock_guard lock{mutex};
    acquired_paths.emplace(std::pair{path_, name_}, std::nullopt);
    return true;
}

void RestoreCoordinationLocal::setResult(const String & zk_path_, const String & name_, Result res_)
{
    std::lock_guard lock{mutex};
    getResultRef(zk_path_, name_) = res_;
    result_changed.notify_all();
}

bool RestoreCoordinationLocal::waitForResult(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const
{
    std::unique_lock lock{mutex};
    auto value = getResultRef(zk_path_, name_);
    if (value)
    {
        res_ = *value;
        return true;
    }

    bool waited = result_changed.wait_for(lock, timeout_, [this, zk_path_, name_] { return getResultRef(zk_path_, name_).has_value(); });
    if (!waited)
        return false;

    res_ = *getResultRef(zk_path_, name_);
    return true;
}

std::optional<IRestoreCoordination::Result> & RestoreCoordinationLocal::getResultRef(const String & zk_path_, const String & name_)
{
    auto it = acquired_paths.find(std::pair{zk_path_, name_});
    if (it == acquired_paths.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path ({}, {}) is not acquired", zk_path_, name_);
    return it->second;
}

const std::optional<IRestoreCoordination::Result> & RestoreCoordinationLocal::getResultRef(const String & zk_path_, const String & name_) const
{
    auto it = acquired_paths.find(std::pair{zk_path_, name_});
    if (it == acquired_paths.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path ({}, {}) is not acquired", zk_path_, name_);
    return it->second;
}

}
