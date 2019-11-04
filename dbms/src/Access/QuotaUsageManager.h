#pragma once

#include <Access/QuotaUsageContext.h>
#include <Access/IAccessStorage.h>
#include <pcg_random.hpp>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace DB
{
class AccessControlManager;


/// Stores information how much amount of resources have been consumed and how much are left.
/// This class is thread-safe.
class QuotaUsageManager
{
public:
    QuotaUsageManager(const AccessControlManager & access_control_manager_);
    ~QuotaUsageManager();

    QuotaUsageContextPtr getContext(const String & user_name, const Poco::Net::IPAddress & address, const String & client_key, const std::vector<UUID> & quota_ids);
    std::vector<QuotaUsageInfo> getInfo() const;

private:
    using Interval = QuotaUsageContext::Interval;
    using Intervals = QuotaUsageContext::Intervals;

    struct MultipleKeysIntervals
    {
        std::unordered_map<String /* quota key */, std::shared_ptr<const Intervals>> key_to_intervals;
        std::shared_ptr<const Quota> quota;
        IAccessStorage::SubscriptionPtr subscription;
        std::vector<std::weak_ptr<QuotaUsageContext>> contexts;
    };

    std::shared_ptr<const Intervals> findOrBuildIntervalsForContext(const std::shared_ptr<QuotaUsageContext> & context);
    std::shared_ptr<const Quota> tryReadQuota(const UUID & quota_id);
    void quotaChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota);
    void quotaRemoved(const UUID & quota_id);

    const AccessControlManager & access_control_manager;
    mutable std::mutex mutex;
    std::unordered_map<UUID /* quota id */, MultipleKeysIntervals> id_to_mki;
    mutable pcg64 rnd_engine;
};
}
