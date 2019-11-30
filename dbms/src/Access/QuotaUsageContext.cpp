#include <Access/QuotaUsageContext.h>
#include <Access/QuotaUsageManager.h>
#include <Access/AccessControlManager.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/randomSeed.h>
#include <ext/chrono_io.h>
#include <ext/range.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/fill.hpp>
#include <boost/range/algorithm/lower_bound.hpp>
#include <boost/range/algorithm/stable_sort.hpp>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int QUOTA_REQUIRES_CLIENT_KEY;
    extern const int QUOTA_EXPIRED;
}


namespace
{
    std::chrono::system_clock::duration randomDuration(std::chrono::seconds max, pcg64 & rnd_engine)
    {
        auto count = std::chrono::duration_cast<std::chrono::system_clock::duration>(max).count();
        std::uniform_int_distribution<Int64> distribution{0, count - 1};
        return std::chrono::system_clock::duration(distribution(rnd_engine));
    }
}


static constexpr size_t MAX_RESOURCE_TYPE = Quota::MAX_RESOURCE_TYPE;


QuotaUsageInfo::Interval::Interval()
{
    boost::range::fill(used, 0);
    boost::range::fill(max, 0);
}


QuotaUsageInfo::QuotaUsageInfo()
    : quota_id(UUID(UInt128(0))),
      quota_key_type(Quota::KeyType::NONE)
{
}


struct QuotaUsageContext::Interval
{
    mutable std::atomic<ResourceAmount> used[MAX_RESOURCE_TYPE];
    ResourceAmount max[MAX_RESOURCE_TYPE];
    std::chrono::seconds duration;
    bool randomize_interval;
    mutable std::atomic<std::chrono::system_clock::duration> end_of_interval;

    Interval() {}
    Interval(const Interval & src) { *this = src; }

    Interval & operator =(const Interval & src)
    {
        randomize_interval = src.randomize_interval;
        duration = src.duration;
        end_of_interval.store(src.end_of_interval.load());
        for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
        {
            max[resource_type] = src.max[resource_type];
            used[resource_type].store(src.used[resource_type].load());
        }
        return *this;
    }

    std::chrono::system_clock::time_point
    getEndOfInterval(std::chrono::system_clock::time_point current_time, bool * used_counters_reset = nullptr) const
    {
        auto end_loaded = end_of_interval.load();
        auto end = std::chrono::system_clock::time_point{end_loaded};
        if (current_time < end)
        {
            if (used_counters_reset)
                *used_counters_reset = false;
            return end;
        }

        do
        {
            end = end + (current_time - end + duration) / duration * duration;
            if (end_of_interval.compare_exchange_strong(end_loaded, end.time_since_epoch()))
            {
                boost::range::fill(used, 0);
                break;
            }
            end = std::chrono::system_clock::time_point{end_loaded};
        }
        while (current_time >= end);

        if (used_counters_reset)
            *used_counters_reset = true;
        return end;
    }

    struct GreaterByDuration
    {
        bool operator()(const Interval & lhs, const Interval & rhs) { return lhs.duration > rhs.duration; }
    };
};


struct QuotaUsageContext::Intervals
{
    std::vector<Interval> intervals;
    UUID quota_id;
    String quota_name;
    String quota_key;
    Quota::KeyType quota_key_type;

    static std::shared_ptr<const Intervals> build(const UUID & quota_id, const Quota & quota, const String & calculated_key, pcg64 & rnd_engine)
    {
        auto result = std::make_shared<Intervals>();
        result->quota_name = quota.getName();
        result->quota_id = quota_id;
        result->quota_key = calculated_key;
        result->quota_key_type = quota.key_type;
        auto & intervals = result->intervals;
        intervals.reserve(quota.all_limits.size());
        for (const auto & limits : quota.all_limits)
        {
            intervals.emplace_back();
            auto & interval = intervals.back();
            interval.duration = limits.duration;
            std::chrono::system_clock::time_point end_of_interval{};
            interval.randomize_interval = limits.randomize_interval;
            if (limits.randomize_interval)
                end_of_interval += randomDuration(limits.duration, rnd_engine);
            interval.end_of_interval = end_of_interval.time_since_epoch();
            for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
            {
                interval.max[resource_type] = limits.max[resource_type];
                interval.used[resource_type] = 0;
            }
        }

        /// Order intervals by durations from largest to smallest.
        /// To report first about largest interval on what quota was exceeded.
        boost::range::stable_sort(intervals, Interval::GreaterByDuration{});
        return result;
    }

    std::shared_ptr<const Intervals> rebuild(const Quota & quota, pcg64 & rnd_engine) const
    {
        auto result = build(quota_id, quota, quota_key, rnd_engine);

        for (auto & new_interval : result->intervals)
        {
            /// Check if an interval with the same duration is already in use.
            auto it = boost::range::lower_bound(intervals, new_interval, Interval::GreaterByDuration{});
            if ((it == intervals.end()) || (it->duration != new_interval.duration))
                continue;

            /// Found an interval with the same duration, we need to copy its usage information to `result`.
            auto & current_interval = *it;
            for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
            {
                new_interval.used[resource_type].store(current_interval.used[resource_type].load());
                new_interval.end_of_interval.store(current_interval.end_of_interval.load());
            }
        }

        return result;
    }

    static std::shared_ptr<const Intervals> unlimited_quota()
    {
        return std::make_shared<Intervals>();
    }

    void used(ResourceType resource_type, ResourceAmount amount, const String & user_name_, std::chrono::system_clock::time_point current_time, bool check_exceeded) const
    {
        for (const auto & interval : intervals)
        {
            ResourceAmount used = (interval.used[resource_type] += amount);
            ResourceAmount max = interval.max[resource_type];
            if (max == Quota::UNLIMITED)
                continue;
            if (used > max)
            {
                bool used_counters_reset = false;
                std::chrono::system_clock::time_point end_of_interval = interval.getEndOfInterval(current_time, &used_counters_reset);
                if (used_counters_reset)
                {
                    used = (interval.used[resource_type] += amount);
                    if ((used > max) && check_exceeded)
                        throwQuotaExceed(user_name_, resource_type, used, max, interval.duration, end_of_interval);
                }
                else if (check_exceeded)
                    throwQuotaExceed(user_name_, resource_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    void checkExceeded(ResourceType resource_type, const String & user_name_, std::chrono::system_clock::time_point current_time) const
    {
        for (const auto & interval : intervals)
        {
            ResourceAmount used = interval.used[resource_type];
            ResourceAmount max = interval.max[resource_type];
            if (max == Quota::UNLIMITED)
                continue;
            if (used > max)
            {
                bool used_counters_reset = false;
                std::chrono::system_clock::time_point end_of_interval = interval.getEndOfInterval(current_time, &used_counters_reset);
                if (!used_counters_reset)
                    throwQuotaExceed(user_name_, resource_type, used, max, interval.duration, end_of_interval);
            }
        }
    }

    void checkExceeded(const String & user_name_, std::chrono::system_clock::time_point current_time) const
    {
        for (auto resource_type : ext::range_with_static_cast<Quota::ResourceType>(Quota::MAX_RESOURCE_TYPE))
            checkExceeded(resource_type, user_name_, current_time);
    }

    [[noreturn]] void throwQuotaExceed(const String & user_name_, ResourceType resource_type, ResourceAmount used, ResourceAmount max, std::chrono::seconds duration, std::chrono::system_clock::time_point end_of_interval) const
    {
        std::stringstream message;
        message << "Quota for user '" << user_name_ << "' for ";

        std::function<String(UInt64)> amount_to_string = [](UInt64 amount) { return std::to_string(amount); };
        if (resource_type == Quota::EXECUTION_TIME)
            amount_to_string = [&](UInt64 amount) { return ext::to_string(std::chrono::nanoseconds(amount)); };

        throw Exception(
            "Quota for user " + backQuote(user_name_) + " for " + ext::to_string(duration) + " has been exceeded: "
                + Quota::getNameOfResourceType(resource_type) + " = " + amount_to_string(used) + "/" + amount_to_string(max) + ". "
                + "Interval will end at " + ext::to_string(end_of_interval) + ". " + "Name of quota template: " + backQuote(quota_name),
            ErrorCodes::QUOTA_EXPIRED);
    }

    QuotaUsageInfo getInfo(std::chrono::system_clock::time_point current_time) const
    {
        QuotaUsageInfo info;
        info.quota_id = quota_id;
        info.quota_name = quota_name;
        info.quota_key = quota_key;
        info.quota_key_type = quota_key_type;
        info.intervals.reserve(intervals.size());
        for (const auto & in : intervals)
        {
            info.intervals.push_back({});
            auto & out = info.intervals.back();
            out.duration = in.duration;
            out.randomize_interval = in.randomize_interval;
            out.end_of_interval = in.getEndOfInterval(current_time);
            for (auto resource_type : ext::range(MAX_RESOURCE_TYPE))
            {
                out.max[resource_type] = in.max[resource_type];
                out.used[resource_type] = in.used[resource_type];
            }
        }
        return info;
    }
};


QuotaUsageContext::QuotaUsageContext()
    : atomic_intervals(Intervals::unlimited_quota())
{
}


QuotaUsageContext::QuotaUsageContext(
    const String & user_name_,
    const Poco::Net::IPAddress & address_,
    const String & client_key_)
    : user_name(user_name_), address(address_), client_key(client_key_)
{
}


QuotaUsageContext::~QuotaUsageContext()
{
}


void QuotaUsageContext::used(ResourceType resource_type, ResourceAmount amount, bool check_exceeded)
{
    used({resource_type, amount}, check_exceeded);
}


void QuotaUsageContext::used(const std::pair<ResourceType, ResourceAmount> & resource, bool check_exceeded)
{
    auto intervals_ptr = std::atomic_load(&atomic_intervals);
    auto current_time = std::chrono::system_clock::now();
    intervals_ptr->used(resource.first, resource.second, user_name, current_time, check_exceeded);
}


void QuotaUsageContext::used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, bool check_exceeded)
{
    auto intervals_ptr = std::atomic_load(&atomic_intervals);
    auto current_time = std::chrono::system_clock::now();
    intervals_ptr->used(resource1.first, resource1.second, user_name, current_time, check_exceeded);
    intervals_ptr->used(resource2.first, resource2.second, user_name, current_time, check_exceeded);
}


void QuotaUsageContext::used(const std::pair<ResourceType, ResourceAmount> & resource1, const std::pair<ResourceType, ResourceAmount> & resource2, const std::pair<ResourceType, ResourceAmount> & resource3, bool check_exceeded)
{
    auto intervals_ptr = std::atomic_load(&atomic_intervals);
    auto current_time = std::chrono::system_clock::now();
    intervals_ptr->used(resource1.first, resource1.second, user_name, current_time, check_exceeded);
    intervals_ptr->used(resource2.first, resource2.second, user_name, current_time, check_exceeded);
    intervals_ptr->used(resource3.first, resource3.second, user_name, current_time, check_exceeded);
}


void QuotaUsageContext::used(const std::vector<std::pair<ResourceType, ResourceAmount>> & resources, bool check_exceeded)
{
    auto intervals_ptr = std::atomic_load(&atomic_intervals);
    auto current_time = std::chrono::system_clock::now();
    for (const auto & resource : resources)
        intervals_ptr->used(resource.first, resource.second, user_name, current_time, check_exceeded);
}


void QuotaUsageContext::checkExceeded()
{
    std::atomic_load(&atomic_intervals)->checkExceeded(user_name, std::chrono::system_clock::now());
}


void QuotaUsageContext::checkExceeded(ResourceType resource_type)
{
    std::atomic_load(&atomic_intervals)->checkExceeded(resource_type, user_name, std::chrono::system_clock::now());
}


bool QuotaUsageContext::canUseQuota(const Quota & quota) const
{
    if (quota.except_roles.count(user_name))
        return false;

    if (quota.all_roles)
        return true;

    return quota.roles.count(user_name);
}


String QuotaUsageContext::calculateKey(const Quota & quota) const
{
    using KeyType = Quota::KeyType;
    switch (quota.key_type)
    {
        case KeyType::NONE:
            return "";
        case KeyType::USER_NAME:
            return user_name;
        case KeyType::IP_ADDRESS:
            return address.toString();
        case KeyType::CLIENT_KEY:
        {
            if (!client_key.empty())
                return client_key;
            throw Exception(
                "Quota " + quota.getName() + " (for user " + user_name + ") requires a client supplied key.",
                ErrorCodes::QUOTA_REQUIRES_CLIENT_KEY);
        }
        case KeyType::CLIENT_KEY_OR_USER_NAME:
        {
            if (!client_key.empty())
                return client_key;
            return user_name;
        }
        case KeyType::CLIENT_KEY_OR_IP_ADDRESS:
        {
            if (!client_key.empty())
                return client_key;
            return address.toString();
        }
    }
    __builtin_unreachable();
}


QuotaUsageInfo QuotaUsageContext::getInfo() const
{
    return std::atomic_load(&atomic_intervals)->getInfo(std::chrono::system_clock::now());
}


QuotaUsageManager::QuotaUsageManager(const AccessControlManager & access_control_manager_)
    : access_control_manager(access_control_manager_), rnd_engine{randomSeed()}
{
}


QuotaUsageManager::~QuotaUsageManager()
{
}


std::shared_ptr<QuotaUsageContext> QuotaUsageManager::getContext(const String & user_name, const Poco::Net::IPAddress & address, const String & client_key)
{
    std::lock_guard lock{mutex};
    ensureAllQuotasRead();
    auto context = ext::shared_ptr_helper<QuotaUsageContext>::create(user_name, address, client_key);
    contexts.push_back(context);
    chooseQuotaForContext(context);
    return context;
}


void QuotaUsageManager::ensureAllQuotasRead()
{
    /// `mutex` is already locked.
    if (all_quotas_read)
        return;
    all_quotas_read = true;
    subscription = access_control_manager.subscribeForChanges<Quota>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                quotaAddedOrChanged(id, typeid_cast<QuotaPtr>(entity));
            else
                quotaRemoved(id);
        });

    for (const UUID & id : access_control_manager.findAll<Quota>())
    {
        auto quota = access_control_manager.tryRead<Quota>(id);
        if (quota)
            all_quotas.emplace(id, QuotaWithIntervals{quota, {}}).first;
    }
}


void QuotaUsageManager::quotaAddedOrChanged(const UUID & quota_id, const std::shared_ptr<const Quota> & new_quota)
{
    std::lock_guard lock{mutex};
    auto it = all_quotas.find(quota_id);
    if (it == all_quotas.end())
    {
        all_quotas.emplace(quota_id, QuotaWithIntervals{new_quota, {}});
        return;
    }
    auto & quota_with_intervals = it->second;
    if (quota_with_intervals.quota == new_quota)
        return;

    quota_with_intervals.quota = new_quota;

    auto & key_to_intervals = quota_with_intervals.key_to_intervals;
    for (auto & intervals : key_to_intervals | boost::adaptors::map_values)
        intervals = intervals->rebuild(*new_quota, rnd_engine);

    chooseQuotaForAllContexts();
}


void QuotaUsageManager::quotaRemoved(const UUID & quota_id)
{
    std::lock_guard lock{mutex};
    all_quotas.erase(quota_id);
    chooseQuotaForAllContexts();
}


void QuotaUsageManager::chooseQuotaForAllContexts()
{
    /// `mutex` is already locked.
    boost::range::remove_erase_if(
        contexts,
        [&](const std::weak_ptr<QuotaUsageContext> & weak)
        {
            auto context = weak.lock();
            if (!context)
                return true; // remove from the `contexts` list.
            chooseQuotaForContext(context);
            return false; // keep in the `contexts` list.
        });
}

void QuotaUsageManager::chooseQuotaForContext(const std::shared_ptr<QuotaUsageContext> & context)
{
    /// `mutex` is already locked.
    std::shared_ptr<const Intervals> intervals;
    for (auto & [quota_id, quota_with_intervals] : all_quotas)
    {
        const auto & quota = *quota_with_intervals.quota;
        if (context->canUseQuota(quota))
        {
            auto & key_to_intervals = quota_with_intervals.key_to_intervals;
            String key = context->calculateKey(quota);
            auto it = key_to_intervals.find(key);
            if (it == key_to_intervals.end())
                it = key_to_intervals.try_emplace(key, Intervals::build(quota_id, quota, key, rnd_engine)).first;
            intervals = it->second;
            break;
        }
    }

    if (!intervals)
        intervals = Intervals::unlimited_quota(); /// No quota == no limits.

    std::atomic_store(&context->atomic_intervals, intervals);
}


std::vector<QuotaUsageInfo> QuotaUsageManager::getInfo() const
{
    std::lock_guard lock{mutex};
    std::vector<QuotaUsageInfo> all_infos;
    auto current_time = std::chrono::system_clock::now();
    for (const auto & info : all_quotas | boost::adaptors::map_values)
    {
        for (const auto & intervals : info.key_to_intervals | boost::adaptors::map_values)
            all_infos.push_back(intervals->getInfo(current_time));
    }
    return all_infos;
}
}
