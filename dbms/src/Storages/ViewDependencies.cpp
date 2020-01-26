#include <Storages/ViewDependencies.h>
#include <Common/Exception.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


ViewDependencies::ViewDependencies() {}
ViewDependencies::~ViewDependencies() {}


void ViewDependencies::add(const StorageID & table, const StorageID & view)
{
    NotificationList notify_list;

    {
        std::lock_guard lock{mutex};

        auto & views = dependencies[table];
        if (views.contains(view))
            throw Exception("Dependency " + view.getFullTableName() + " on " + table.getFullTableName() + " already exists",
                            ErrorCodes::LOGICAL_ERROR);

        views.emplace(view);

        notify_list = getNotificationList();
    }

    notify(notify_list, table, view, true);
}


void ViewDependencies::remove(const StorageID & table, const StorageID & view)
{
    NotificationList notify_list;

    {
        std::lock_guard lock{mutex};
        auto it = dependencies.find(table);
        if (it == dependencies.end())
            return;

        auto & views = it->second;
        auto it2 = views.find(view);
        if (it2 == views.end())
            return;

        views.erase(it2);
        if (views.empty())
            dependencies.erase(it);

        notify_list = getNotificationList();
    }

    notify(notify_list, table, view, false);
}


bool ViewDependencies::has(const StorageID & table, const StorageID & view) const
{
    std::lock_guard lock{mutex};
    auto it = dependencies.find(table);
    if (it == dependencies.end())
        return false;

    const auto & views = it->second;
    return views.contains(view);
}


bool ViewDependencies::hasViews(const StorageID & table) const
{
    std::lock_guard lock{mutex};
    auto it = dependencies.find(table);
    if (it == dependencies.end())
        return {};

    const auto & views = it->second;
    return !views.empty();
}

std::vector<StorageID> ViewDependencies::getViews(const StorageID & table) const
{
    std::lock_guard lock{mutex};
    auto it = dependencies.find(table);
    if (it == dependencies.end())
        return {};

    const auto & views = it->second;
    return boost::copy_range<std::vector<StorageID>>(views);
}


ext::scope_guard ViewDependencies::subscribeForChanges(const OnChangedHandler & handler) const
{
    std::lock_guard lock{mutex};
    subscriptions.push_back(handler);
    auto it = std::prev(subscriptions.end());

    return [it, this]()
    {
        std::lock_guard unsubscribe_lock{mutex};
        subscriptions.erase(it);
    };
}

ViewDependencies::NotificationList ViewDependencies::getNotificationList() const
{
    return {subscriptions.begin(), subscriptions.end()};
}


void ViewDependencies::notify(const NotificationList & notification_list, const StorageID & table, const StorageID & view, bool added)
{
    for (const auto & handler : notification_list)
        handler(table, view, added);
}

}

