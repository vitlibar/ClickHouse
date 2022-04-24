#pragma once

#include <Backups/IRestoreCoordination.h>
#include <map>


namespace DB
{

class RestoreCoordinationLocal : public IRestoreCoordination
{
public:
    RestoreCoordinationLocal();
    ~RestoreCoordinationLocal() override;

    bool acquirePath(const String & zk_path_, const String & name_) override;
    void setResult(const String & zk_path_, const String & name_, Result res_) override;
    bool waitForResult(const String & zk_path_, const String & name_, Result & res_, std::chrono::milliseconds timeout_) const override;

private:
    std::optional<Result> & getResultRef(const String & zk_path_, const String & name_);
    const std::optional<Result> & getResultRef(const String & zk_path_, const String & name_) const;

    mutable std::mutex mutex;
    mutable std::condition_variable result_changed;
    std::map<std::pair<String, String>, std::optional<Result>> acquired_paths;
};

}
