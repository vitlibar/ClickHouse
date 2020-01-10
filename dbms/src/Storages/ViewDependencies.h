#pragma once

#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <list>
#include <mutex>
#include <unordered_map>


namespace DB
{
    /// Keeps dependencies of views on tables or other views.
    class ViewDependencies
    {
    public:
        using DatabaseAndTableName = std::pair<String, String>;
        using DatabaseAndTableNameRef = std::pair<std::string_view, std::string_view>;
        using DatabaseAndTableAndColumnNames = std::tuple<String, String, Strings>;

        ViewDependencies();
        ~ViewDependencies();

        /// Adds a dependency of `target` on `source`.
        ext::scope_guard addDependency(const DatabaseAndTableAndColumnNames & source, const DatabaseAndTableNameRef & target);

        /// Returns all the sources of a specified `target`.
        std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> getSources(const DatabaseAndTableNameRef & target) const;

        /// Returns all the targets of a specified `source`.
        std::shared_ptr<const std::vector<DatabaseAndTableName>> getTargets(const DatabaseAndTableNameRef & source) const;

        /// Returns all the sources of a specified `target` recursively,
        /// i.e. it returns all the direct sources, the sources of the sources, and so on.
        std::shared_ptr<const std::vector<DatabaseAndTableAndColumnNames>> getSourcesRecursively(const DatabaseAndTableNameRef & target) const;

        using OnChangedHandler = std::function<void(const DatabaseAndTableName & source)>;

        /// Registers a handler which will be called on any change of the dependencies.
        ext::scope_guard subscribeForChanges(const OnChangedHandler & handler) const;

    private:
        struct Entry;

        struct Hash
        {
            size_t operator()(const DatabaseAndTableNameRef &) const;
        };

        using Map = std::unordered_map<DatabaseAndTableNameRef, std::unique_ptr<Entry>, Hash>;

        void removeDependency(const DatabaseAndTableName & source, const DatabaseAndTableName & target);

        const Entry * tryGetEntry(const DatabaseAndTableNameRef & db_and_table_name) const;
        Entry * tryGetEntry(const DatabaseAndTableNameRef & db_and_table_name);
        Entry & getEntry(const DatabaseAndTableNameRef & db_and_table_name);
        void removeEntryIfNotNeed(Entry & entry);

        using NotificationList = std::pair<std::vector<OnChangedHandler>, DatabaseAndTableName>;
        NotificationList prepareNotificationList(const DatabaseAndTableName & source) const;
        static void notify(const NotificationList & notification_list);

        Map map;
        mutable std::list<OnChangedHandler> subscriptions;
        mutable std::mutex mutex;
   };
}
