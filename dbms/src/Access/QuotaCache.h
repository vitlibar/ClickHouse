#pragma once

#include <Access/EnabledQuota.h>
#include <ext/scope_guard.h>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
class AccessControlManager;


/// Stores information how much amount of resources have been consumed and how much are left.
class QuotaCache
{
public:
    QuotaCache(const AccessControlManager & access_control_manager_);
    ~QuotaCache();

    std::shared_ptr<const EnabledQuota> getEnabledQuota(const String & user_name, const UUID & user_id, const std::vector<UUID> & enabled_roles, const Poco::Net::IPAddress & address, const String & client_key);
    std::vector<QuotaUsageInfo> getUsageInfo() const;

private:
    using Interval = EnabledQuota::Interval;
    using Intervals = EnabledQuota::Intervals;

    struct QuotaInfo
    {
        QuotaInfo(const QuotaPtr & quota_, const UUID & quota_id_) { setQuota(quota_, quota_id_); }
        void setQuota(const QuotaPtr & quota_, const UUID & quota_id_);

        String calculateKey(const EnabledQuota & enabled_quota) const;
        boost::shared_ptr<const Intervals> getOrBuildIntervals(const String & key);
        boost::shared_ptr<const Intervals> rebuildIntervals(const String & key);
        void rebuildAllIntervals();

        QuotaPtr quota;
        UUID quota_id;
        const GeneralizedRoleSet * roles = nullptr;
        std::unordered_map<String /* quota key */, boost::shared_ptr<const Intervals>> key_to_intervals;
    };

    void ensureAllQuotasRead();
    void quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota);
    void quotaRemoved(const UUID & quota_id);
    void chooseQuotaToConsume();
    void chooseQuotaToConsumeFor(EnabledQuota & enabled_quota);

    const AccessControlManager & access_control_manager;
    mutable std::mutex mutex;
    std::unordered_map<UUID /* quota id */, QuotaInfo> all_quotas;
    bool all_quotas_read = false;
    ext::scope_guard subscription;
    std::vector<std::weak_ptr<EnabledQuota>> enabled_quotas;
};
}
