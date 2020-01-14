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

        using Lock = std::unique_lock<std::recursive_mutex>;
        Lock getLock() const { return std::unique_lock{mutex}; }

        /// Adds a dependency of `view` on `table`.
        /// `table` is where `view` get data from, in fact it can be another view.
        void add(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns);
        void remove(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view);

        /// Whether a specified dependency exists.
        bool has(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view) const;

        /// Calls a specified function for each dependency.
        /// `order_by_dependency` specifies if we want to get dependencies in their correct order,
        /// for example, if there were the view V1 for the table T and the view V2 for the view V1
        /// then the correct order of dependencies would be T->V1, V1->V2.
        void forEach(const std::function<void(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns)> & fn, bool order_by_dependency = false) const;

        /// Returns list of the table columns for a specified dependency.
        Strings getTableColumns(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view) const;

        /// Returns all views for a specified table.
        bool hasViews(const DatabaseAndTableNameRef & table) const;
        std::vector<DatabaseAndTableName> getViews(const DatabaseAndTableNameRef & table) const;

        /// Registers a handler which will be called on any change of the dependencies.
        using OnChangedHandler = std::function<void(const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & table_columns /* empty list when called after removing a dependency */)>;
        ext::scope_guard subscribeForChanges(const OnChangedHandler & handler) const;

    private:
        struct Hash
        {
            size_t operator()(const DatabaseAndTableNameRef & database_and_table_name) const;
        };

        struct ViewEntry
        {
            std::unique_ptr<DatabaseAndTableName> name_keeper;
            Strings table_columns;
        };

        struct TableEntry
        {
            std::unique_ptr<DatabaseAndTableName> name_keeper;
            std::unordered_map<DatabaseAndTableNameRef, ViewEntry, Hash> views;
        };

        std::unordered_map<DatabaseAndTableNameRef, TableEntry, Hash> tables;

        using NotificationList = std::vector<OnChangedHandler>;
        NotificationList getNotificationList() const;
        static void notify(const NotificationList & notification_list, const DatabaseAndTableNameRef & table, const DatabaseAndTableNameRef & view, const Strings & columns);

        mutable std::list<OnChangedHandler> subscriptions;
        mutable std::recursive_mutex mutex;
    };
}
