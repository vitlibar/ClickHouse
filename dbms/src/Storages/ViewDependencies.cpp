#include <Storages/ViewDependencies.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    using DatabaseAndTableName = ViewDependencies::DatabaseAndTableName;
    using DatabaseAndTableNameRef = ViewDependencies::DatabaseAndTableNameRef;
    using DatabaseAndTableAndColumnNames = ViewDependencies::DatabaseAndTableAndColumnNames;

    String toString(const DatabaseAndTableNameRef & db_and_table_name)
    {
        String str;
        if (!db_and_table_name.first.empty())
            str += backQuoteIfNeed(db_and_table_name.first) + ".";
        str += backQuoteIfNeed(db_and_table_name.second);
        return str;
    }

    auto find(const std::vector<DatabaseAndTableName> & vec, const DatabaseAndTableNameRef & x)
    {
        return std::find_if(vec.begin(), vec.end(), [&x](const DatabaseAndTableName & y)
        {
            return (x.first == y.first) && (x.second == y.second);
        });
    }

    auto find(const std::vector<DatabaseAndTableAndColumnNames> & vec, const DatabaseAndTableNameRef & x)
    {
        return std::find_if(vec.begin(), vec.end(), [&x](const DatabaseAndTableAndColumnNames & y)
        {
            return (x.first == std::get<0>(y)) && (x.second == std::get<1>(y));
        });
    }

    template <typename T>
    void appendTo(std::shared_ptr<const std::vector<T>> & vec, T new_element)
    {
        auto new_vec = std::make_shared<std::vector<T>>();
        if (vec)
            new_vec->assign(vec->begin(), vec->end());
        new_vec->push_back(std::move(new_element));
        vec = std::move(new_vec);
    }

    template <typename T, typename Iter>
    void erase(std::shared_ptr<const std::vector<T>> & vec, const Iter & it)
    {
        const auto & old_vec = *vec;
        if ((old_vec.size() == 1) && (old_vec.begin() == it))
        {
            vec = nullptr;
            return;
        }
        auto new_vec = std::make_shared<std::vector<T>>();
        new_vec->reserve(old_vec.size() - 1);
        new_vec->assign(old_vec.begin(), it);
        new_vec->insert(new_vec->end(), std::next(it), old_vec.end());
        vec = std::move(new_vec);
    }

    void eraseByValue(std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> & vec,
                      const DatabaseAndTableNameRef & x)
    {
        erase(vec, find(*vec, x));
    }
}


struct ViewDependencies::Entry
{
    DatabaseAndTableName database_and_table_name;
    std::shared_ptr<const std::vector<DatabaseAndTableName>> targets;
    std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> sources;
    std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> sources_recursively;
};


size_t ViewDependencies::Hash::operator()(const DatabaseAndTableNameRef & database_and_table_name) const
{
    return std::hash<std::string_view>{}(database_and_table_name.first) - std::hash<std::string_view>{}(database_and_table_name.second);
}


ViewDependencies::ViewDependencies() {}
ViewDependencies::~ViewDependencies() {}


ext::scope_guard ViewDependencies::addDependency(const DatabaseAndTableAndColumnNames & source, const DatabaseAndTableNameRef & target)
{
    DatabaseAndTableName source_db_and_table{std::get<0>(source), std::get<1>(source)};
    std::optional<NotificationList> notify_list;

    {
        std::lock_guard lock{mutex};
        auto & source_entry = getEntry(source_db_and_table);
        auto & target_entry = getEntry(target);

        if (source_entry.targets && find(*source_entry.targets, target) != source_entry.targets->end())
            throw Exception("Dependency " + toString(target) + " on " + toString(source_db_and_table) + " already exists",
                            ErrorCodes::LOGICAL_ERROR);

        appendTo(source_entry.targets, DatabaseAndTableName{target});
        appendTo(target_entry.sources, source);

        static constexpr void (*add_to_source_rec)(Map &, const DatabaseAndTableAndColumnNames &, Entry &)
            = [](Map & map_, const DatabaseAndTableAndColumnNames & source_, Entry & target_)
        {
            appendTo(target_.sources_recursively, source_);
            if (target_.targets)
                for (const auto & target_of_target : *target_.targets)
                    add_to_source_rec(map_, source_, *map_[target_of_target]);
        };
        add_to_source_rec(map, source, target_entry);

        notify_list = prepareNotificationList(source_db_and_table);
    }

    notify(*notify_list);

    return std::bind(&ViewDependencies::removeDependency, this, source_db_and_table, DatabaseAndTableName{target});
}


void ViewDependencies::removeDependency(const DatabaseAndTableName & source, const DatabaseAndTableName & target)
{
    std::optional<NotificationList> notify_list;

    {
        std::lock_guard lock{mutex};
        auto * source_entry = tryGetEntry(source);
        auto * target_entry = tryGetEntry(target);

        if (!source_entry || !target_entry || !source_entry->targets)
            throw Exception("Dependency " + toString(target) + " on " + toString(source) + " doesn't exist",
                            ErrorCodes::LOGICAL_ERROR);

        auto it = find(*source_entry->targets, target);
        if (it == source_entry->targets->end())
            throw Exception("Dependency " + toString(target) + " on " + toString(source) + " doesn't exist",
                            ErrorCodes::LOGICAL_ERROR);

        erase(source_entry->targets, it);
        eraseByValue(target_entry->sources, source);

        static constexpr void (*remove_from_source_rec)(Map &, const DatabaseAndTableName &, Entry &)
            = [](Map & map_, const DatabaseAndTableName & source_, Entry & target_)
        {
            eraseByValue(target_.sources_recursively, source_);
            if (target_.targets)
                for (const auto & target_of_target : *target_.targets)
                    remove_from_source_rec(map_, source_, *map_[target_of_target]);
        };
        remove_from_source_rec(map, source, *target_entry);

        removeEntryIfNotNeed(*source_entry);
        removeEntryIfNotNeed(*target_entry);

        notify_list = prepareNotificationList(source);
    }

    notify(*notify_list);
}


std::shared_ptr<const std::vector<DatabaseAndTableName>> ViewDependencies::getTargets(const DatabaseAndTableNameRef & source) const
{
    std::lock_guard lock{mutex};
    const auto * entry = tryGetEntry(source);
    return entry ? entry->targets : nullptr;
}

std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> ViewDependencies::getSources(const DatabaseAndTableNameRef & target) const
{
    std::lock_guard lock{mutex};
    const auto * entry = tryGetEntry(target);
    return entry ? entry->sources : nullptr;
}

std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> ViewDependencies::getSourcesRecursively(const DatabaseAndTableNameRef & target) const
{
    std::lock_guard lock{mutex};
    const auto * entry = tryGetEntry(target);
    return entry ? entry->sources_recursively : nullptr;
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


const ViewDependencies::Entry * ViewDependencies::tryGetEntry(const DatabaseAndTableNameRef & db_and_table_name) const
{
    auto it = map.find(db_and_table_name);
    if (it != map.end())
        return it->second.get();
    return nullptr;
}


ViewDependencies::Entry * ViewDependencies::tryGetEntry(const DatabaseAndTableNameRef & db_and_table_name)
{
    auto it = map.find(db_and_table_name);
    if (it != map.end())
        return it->second.get();
    return nullptr;
}


ViewDependencies::Entry & ViewDependencies::getEntry(const DatabaseAndTableNameRef & db_and_table_name)
{
    auto it = map.find(db_and_table_name);
    if (it != map.end())
        return *(it->second);
    auto new_entry = std::make_unique<Entry>();
    new_entry->database_and_table_name = db_and_table_name;
    DatabaseAndTableNameRef new_key = new_entry->database_and_table_name;
    return *(map.emplace(new_key, std::move(new_entry)).first->second);
}


void ViewDependencies::removeEntryIfNotNeed(Entry & entry)
{
    if (entry.sources && !entry.sources->empty())
        return;
    if (entry.targets && !entry.targets->empty())
        return;
    map.erase(entry.database_and_table_name);
}


ViewDependencies::NotificationList ViewDependencies::prepareNotificationList(const DatabaseAndTableName & source) const
{
    return {{subscriptions.begin(), subscriptions.end()}, source};
}


void ViewDependencies::notify(const NotificationList & notification_list)
{
    const auto & db_and_table_name = notification_list.second;
    for (const auto & handler : notification_list.first)
        handler(db_and_table_name);
}

}
