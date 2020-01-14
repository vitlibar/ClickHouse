#pragma once

#include <Core/Types.h>
#include <ext/scope_guard.h>
#include <list>
#include <mutex>
#include <unordered_map>


namespace DB
{
    using DatabaseAndTableName = std::pair<String, String>;
    using DatabaseAndTableNameRef = std::pair<std::string_view, std::string_view>;

    /// Keeps dependencies of views on tables or other views.
    class ViewDependencies
    {
    public:
        ViewDependencies();
        ~ViewDependencies();

        /// Adds a dependency of `view` on `table`.
        /// `table` is where `view` get data from, in fact it can be another view.
        void add(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view);
        void remove(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view);

        /// Whether a specified dependency exists.
        bool has(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view) const;

        /// Returns all views for a specified table.
        bool hasViews(const DatabaseAndTableNameRef & table) const;
        std::vector<DatabaseAndTableName> getViews(const DatabaseAndTableNameRef & table) const;

        /// Registers a handler which will be called on any change of the dependencies.
        using OnChangedHandler = std::function<void(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, bool added)>;
        ext::scope_guard subscribeForChanges(const OnChangedHandler & handler) const;

        using Lock = std::unique_lock<std::recursive_mutex>;
        Lock getLock() const { return std::unique_lock{mutex}; }

    private:
        struct Hash
        {
            size_t operator()(const DatabaseAndTableNameRef & database_and_table_name) const;
        };

        struct ViewEntry
        {
            std::unique_ptr<DatabaseAndTableName> name_keeper;
        };

        struct TableEntry
        {
            std::unique_ptr<DatabaseAndTableName> name_keeper;
            std::unordered_map<DatabaseAndTableNameRef, ViewEntry, Hash> views;
        };

        std::unordered_map<DatabaseAndTableNameRef, TableEntry, Hash> tables;

        using NotificationList = std::vector<OnChangedHandler>;
        NotificationList getNotificationList() const;
        static void notify(const NotificationList & notification_list, const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, bool added);

        mutable std::list<OnChangedHandler> subscriptions;
        mutable std::recursive_mutex mutex;
    };
}
