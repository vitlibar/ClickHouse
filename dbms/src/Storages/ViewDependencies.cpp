#include <Storages/ViewDependencies.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    String toString(const DatabaseAndTableNameRef & db_and_table_name)
    {
        String str;
        if (!db_and_table_name.first.empty())
            str += backQuoteIfNeed(db_and_table_name.first) + ".";
        str += backQuoteIfNeed(db_and_table_name.second);
        return str;
    }
}


size_t ViewDependencies::Hash::operator()(const DatabaseAndTableNameRef & database_and_table_name) const
{
    return std::hash<std::string_view>{}(database_and_table_name.first) - std::hash<std::string_view>{}(database_and_table_name.second);
}


ViewDependencies::ViewDependencies() {}
ViewDependencies::~ViewDependencies() {}


void ViewDependencies::add(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns)
{
    if (table_columns.empty())
        return;

    NotificationList notify_list;

    {
        std::lock_guard lock{mutex};

        auto it = tables.find(table);
        if (it == tables.end())
        {
            TableEntry new_table_entry;
            new_table_entry.name_keeper = std::make_unique<DatabaseAndTableName>(table);
            DatabaseAndTableNameRef table_name = *new_table_entry.name_keeper;
            it = tables.emplace(table_name, std::move(new_table_entry)).first;
        }

        TableEntry & table_entry = it->second;
        auto it2 = table_entry.views.find(view);
        if (it2 != table_entry.views.end())
            throw Exception("Dependency " + toString(view) + " on " + toString(table) + " already exists",
                            ErrorCodes::LOGICAL_ERROR);

        ViewEntry view_entry;
        view_entry.name_keeper = std::make_unique<DatabaseAndTableName>(view);
        view_entry.table_columns = table_columns;
        DatabaseAndTableNameRef view_name = *view_entry.name_keeper;
        table_entry.views.emplace(view_name, std::move(view_entry));

        notify_list = getNotificationList();
    }

    notify(notify_list, table, view, table_columns);
}


void ViewDependencies::remove(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view)
{
    NotificationList notify_list;

    {
        std::lock_guard lock{mutex};
        auto it = tables.find(table);
        if (it == tables.end())
            return;

        auto & table_entry = it->second;
        auto it2 = table_entry.views.find(view);
        if (it2 == table_entry.views.end())
            return;

        table_entry.views.erase(it2);
        if (table_entry.views.empty())
            tables.erase(it);

        notify_list = getNotificationList();
    }

    notify(notify_list, table, view, {});
}


bool ViewDependencies::has(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view) const
{
    std::lock_guard lock{mutex};
    auto it = tables.find(table);
    if (it == tables.end())
        return false;

    auto & table_entry = it->second;
    auto it2 = table_entry.views.find(view);
    if (it2 == table_entry.views.end())
        return false;

    return true;
}


void ViewDependencies::forEach(const std::function<void(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns)> & fn,
                               bool order_by_dependency) const
{
    std::lock_guard lock{mutex};

    if (!order_by_dependency)
    {
        for (const auto & [table_name, table_entry] : tables)
            for (const auto & [view_name, view_entry] : table_entry.views)
                fn(table_name, view_name, view_entry.table_columns);
        return;
    }

    std::vector<std::tuple<DatabaseAndTableNameRef, DatabaseAndTableNameRef, std::reference_wrapper<const Strings>>> all;
    for (const auto & [table_name, table_entry] : tables)
        for (const auto & [view_name, view_entry] : table_entry.views)
            all.emplace_back(table_name, view_name, view_entry.table_columns);

    /// Reorder V1->V2, T->V1 to T->V1, V1->V2.
    for (size_t i = 0; i != all.size(); ++i)
    {
        for (size_t j = 0; j < i; ++j)
        {
            if (std::get<0>(all[j]) == std::get<1>(all[i]))
            {
                std::swap(all[i], all[j]);
                break;
            }
        }
    }

    for (const auto & [table_name, view_name, table_columns] : all)
        fn(table_name, view_name, table_columns);
}


Strings ViewDependencies::getTableColumns(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view) const
{
    std::lock_guard lock{mutex};
    auto it = tables.find(table);
    if (it == tables.end())
        return {};

    const auto & table_entry = it->second;
    auto it2 = table_entry.views.find(view);
    if (it2 == table_entry.views.end())
        return {};

    const auto & view_entry = it2->second;
    return view_entry.table_columns;
}


bool ViewDependencies::hasViews(const DatabaseAndTableNameRef & table) const
{
    std::lock_guard lock{mutex};
    auto it = tables.find(table);
    if (it == tables.end())
        return {};

    const auto & table_entry = it->second;
    return !table_entry.views.empty();
}

std::vector<DatabaseAndTableName> ViewDependencies::getViews(const DatabaseAndTableNameRef & table) const
{
    std::lock_guard lock{mutex};
    auto it = tables.find(table);
    if (it == tables.end())
        return {};

    const auto & table_entry = it->second;
    return boost::copy_range<std::vector<DatabaseAndTableName>>(table_entry.views | boost::adaptors::map_keys);
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


void ViewDependencies::notify(const NotificationList & notification_list, const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns)
{
    for (const auto & handler : notification_list)
        handler(table, view, table_columns);
}

}

