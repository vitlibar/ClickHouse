#pragma once

#include <Access/Quota.h>
#include <Poco/Net/IPAddress.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <vector>


namespace DB
{
class AccessControlManager;


class QuotaConsumption
{
public:
    QuotaConsumption();
    ~QuotaConsumption();
    void consume(ResourceType resource_type, ResourceAmount amount);
    void consume(ResourceType resource_type, ResourceAmount amount, std::chrono::system_clock::time_point current_time);

private:
    struct Interval
    {
        std::chrono::seconds duration;
        std::chrono::system_clock::duration offset;
        std::atomic<ResourceAmount> max[MAX_RESOURCE_TYPE];
        std::atomic<ResourceAmount> used[MAX_RESOURCE_TYPE];
        std::atomic<std::chrono::system_clock::time_point> end_of_interval;
    };
    std::atomic<Intervals *> intervals;
};


/// Consumes quotas and stores information how much amount of resources have been consumed and how mush are left.
/// This class is thread-safe.
class Quotas
{
public:
    using ResourceType = Quota::ResourceType;
    using ResourceAmount = Quota::ResourceAmount;
    using Consumption = QuotaConsumption;

    Quotas(const AccessControlManager & access_control_manager_)
    {
        auto on_changed = [&](const UUID & id,
                              const std::shared_ptr<const Quota> & old_quota,
                              const std::shared_ptr<const Quota> & new_quota)
        {
            if (!new_quota)
            {
                all_quotas.erase(old_quota->name);
                return;
            }

            if (!old_quota)
            {
                all_quotas.insert(new_quota->name);
                return;
            }

            if (new_quota->name != old_quota->name)
            {
                all_quotas.erase
                return;
            }

            all_quotas[new_quota->name].quota = new_quota;
        };

        subscription = access_control_manager_.subscribeForAllChanges<Quota>(on_changed);
        for (const auto & id : access_control_manager_.findAll<Quota>())
        {
            auto quota = access_control_manager_.tryRead<Quota>(id);
            if (quota)
                on_changed(id, nullptr, quota);
        }
    }
    ~Quotas();

    std::shared_ptr<Consumption> prepareToConsume(const String & quota_name, const String & custom_quota_key, const String & user_name, const Poco::Net::IPAddress & address);

private:
    struct QuotaWithKeys
    {
        std::shared_ptr<const Quota> quota;
        std::unordered_map<String, std::shared_ptr<Consumption>> consumption_map;
    };
    std::unordered_map<String, QuotaWithKeys> all_quotas;
    std::unique_ptr<IAttributesStorage::SubscriptionPtr> subscription;
    std::mutex mutex;

    struct Interval;
    class Intervals;
    class ConsumptionMap;
    struct ExceedInfo;

    const std::vector<Quota2> quotas;
    const String user_name;
    const IPAddress ip_address;
    const String custom_consumption_key;

    using AtomicIntervalPtr = std::atomic<Intervals *>;
    using SubscriptionPtr = IAttributesStorage::SubscriptionPtr;

    std::unique_ptr<AtomicIntervalPtr[]> intervals_for_quotas;
    std::unique_ptr<SubscriptionPtr[]> subscriptions;
};

using QuotasConsumerPtr = std::shared_ptr<QuotasConsumer>;
}
