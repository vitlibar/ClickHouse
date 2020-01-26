#pragma once

#include <Storages/StorageID.h>
#include <ext/scope_guard.h>
#include <list>
#include <mutex>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
    /// Keeps dependencies of views on tables or other views.
    class ViewDependencies
    {
    public:
        ViewDependencies();
        ~ViewDependencies();

        /// Adds a dependency of `view` on `table`.
        /// `table` is where `view` get data from, in fact it can be another view.
        void add(const StorageID & table, const StorageID & view);
        void remove(const StorageID & table, const StorageID & view);

        /// Whether a specified dependency exists.
        bool has(const StorageID & table, const StorageID & view) const;

        /// Returns all views for a specified table.
        bool hasViews(const StorageID & table) const;
        std::vector<StorageID> getViews(const StorageID & table) const;

        /// Registers a handler which will be called on any change of the dependencies.
        using OnChangedHandler = std::function<void(const StorageID & table, const StorageID & view, bool added)>;
        ext::scope_guard subscribeForChanges(const OnChangedHandler & handler) const;

        using Lock = std::unique_lock<std::recursive_mutex>;
        Lock getLock() const { return std::unique_lock{mutex}; }

    private:
        std::unordered_map<StorageID /* table */, std::unordered_set<StorageID> /* views */> dependencies;

        using NotificationList = std::vector<OnChangedHandler>;
        NotificationList getNotificationList() const;
        static void notify(const NotificationList & notification_list, const StorageID & table, const StorageID & view, bool added);

        mutable std::list<OnChangedHandler> subscriptions;
        mutable std::recursive_mutex mutex;
    };
}
