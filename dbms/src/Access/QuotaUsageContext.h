#pragma once

#include <Access/Quota.h>
#include <Core/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <ext/shared_ptr_helper.h>
#include <boost/noncopyable.hpp>
#include <chrono>
#include <memory>


namespace DB
{
struct QuotaUsageInfo;


/// Instances of `QuotaUsageContext` are used to track resource consumption.
class QuotaUsageContext : public boost::noncopyable
{
public:
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;

    /// Default constructors makes an unlimited quota.
    QuotaUsageContext();

    ~QuotaUsageContext();

    /// Tracks resource consumption. If the quota exceeded and `check_exceeded == true`, throws an exception.
    void used(ResourceType resource_type, ResourceAmount amount, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, bool check_exceeded = true);
    void used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, const std::pair<ResourceType, ResourceAmount> & resource3, bool check_exceeded = true);
    void used(const std::vector<std::pair<ResourceType, ResourceAmount>> & resources, bool check_exceeded = true);

    /// Checks if the quota exceeded. If so, throws an exception.
    void checkExceeded();
    void checkExceeded(ResourceType resource_type);

    /// Returns the information about this quota usage context.
    QuotaUsageInfo getInfo() const;

private:
    friend class QuotaUsageManager;
    friend struct ext::shared_ptr_helper<QuotaUsageContext>;
    struct Interval;
    struct Intervals;

    /// Instances of this class are created by QuotaUsageManager.
    QuotaUsageContext(const String & user_name_, const Poco::Net::IPAddress & address_, const String & client_key_, const std::vector<UUID> & quota_ids_);

    String calculateKey(const Quota & quota) const;

    const String user_name;
    const Poco::Net::IPAddress address;
    const String client_key;
    const std::vector<UUID> quota_ids;
    std::shared_ptr<const Intervals> atomic_intervals; /// atomically changed by QuotaUsageManager
};

using QuotaUsageContextPtr = std::shared_ptr<QuotaUsageContext>;


/// The information about a quota usage context.
struct QuotaUsageInfo
{
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    static constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;

    struct Interval
    {
        ResourceAmount used[MAX_RESOURCE_TYPE];
        ResourceAmount max[MAX_RESOURCE_TYPE];
        std::chrono::seconds duration = std::chrono::seconds::zero();
        bool randomize_interval = false;
        std::chrono::system_clock::time_point end_of_interval;
        Interval();
    };

    std::vector<Interval> intervals;
    UUID quota_id;
    String quota_name;
    String quota_key;
    Quota::KeyType quota_key_type;
    QuotaUsageInfo();
};
}
