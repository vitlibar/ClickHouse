#include <Access/MultipleAccessStorage.h>
#include <Common/Exception.h>
#include <ext/range.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/algorithm/find.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND;
}

using Storage = IAccessStorage;
using StoragePtr = std::shared_ptr<Storage>;
using StorageConstPtr = std::shared_ptr<const Storage>;
using Storages = std::vector<StoragePtr>;


MultipleAccessStorage::MultipleAccessStorage(const String & storage_name_)
    : IAccessStorage(storage_name_)
    , nested_storages(std::make_shared<Storages>())
    , ids_cache(512 /* cache size */)
{
}


size_t MultipleAccessStorage::getNumberOfStorages() const
{
    std::lock_guard lock{mutex};
    return nested_storages->size();
}

std::shared_ptr<IAccessStorage> MultipleAccessStorage::getStorageByIndex(size_t i)
{
    std::lock_guard lock{mutex};
    return nested_storages->at(i);
}

std::shared_ptr<const IAccessStorage> MultipleAccessStorage::getStorageByIndex(size_t i) const
{
    std::lock_guard lock{mutex};
    return nested_storages->at(i);
}

void MultipleAccessStorage::setStorages(const std::vector<StoragePtr> & storages)
{
    std::unique_lock lock{mutex};
    nested_storages = std::make_shared<const Storages>(storages);
    updateSubscriptionsToStorages(lock);
}

void MultipleAccessStorage::addStorage(const std::shared_ptr<Storage> & new_storage, size_t i)
{
    std::unique_lock lock{mutex};
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    if (i > new_storages->size())
        i = new_storages->size();
    new_storages->insert(new_storages->begin() + i, new_storage);
    nested_storages = new_storages;
    updateSubscriptionsToStorages(lock);
}

void MultipleAccessStorage::removeStorage(size_t i)
{
    std::unique_lock lock{mutex};
    auto new_storages = std::make_shared<Storages>(*nested_storages);
    new_storages->erase(new_storages->begin() + i);
    nested_storages = new_storages;
    ids_cache.reset();
    updateSubscriptionsToStorages(lock);
}

std::shared_ptr<const Storages> MultipleAccessStorage::getStorages() const
{
    std::lock_guard lock{mutex};
    return nested_storages;
}


std::optional<UUID> MultipleAccessStorage::findImpl(EntityType type, const String & name) const
{
    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        auto id = storage->find(type, name);
        if (id)
        {
            std::lock_guard lock{mutex};
            ids_cache.set(*id, storage);
            return id;
        }
    }
    return {};
}


std::vector<UUID> MultipleAccessStorage::findAllImpl(EntityType type) const
{
    std::vector<UUID> all_ids;
    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        auto ids = storage->findAll(type);
        all_ids.insert(all_ids.end(), std::make_move_iterator(ids.begin()), std::make_move_iterator(ids.end()));
    }
    return all_ids;
}


bool MultipleAccessStorage::existsImpl(const UUID & id) const
{
    return findStorage(id) != nullptr;
}


StoragePtr MultipleAccessStorage::findStorage(const UUID & id)
{
    StoragePtr from_cache;
    {
        std::lock_guard lock{mutex};
        from_cache = ids_cache.get(id);
    }
    if (from_cache && from_cache->exists(id))
        return from_cache;

    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        if (storage->exists(id))
        {
            std::lock_guard lock{mutex};
            ids_cache.set(id, storage);
            return storage;
        }
    }

    return nullptr;
}


StorageConstPtr MultipleAccessStorage::findStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->findStorage(id);
}


StoragePtr MultipleAccessStorage::getStorage(const UUID & id)
{
    auto storage = findStorage(id);
    if (storage)
        return storage;
    throwNotFound(id);
}


StorageConstPtr MultipleAccessStorage::getStorage(const UUID & id) const
{
    return const_cast<MultipleAccessStorage *>(this)->getStorage(id);
}

AccessEntityPtr MultipleAccessStorage::readImpl(const UUID & id) const
{
    return getStorage(id)->read(id);
}


String MultipleAccessStorage::readNameImpl(const UUID & id) const
{
    return getStorage(id)->readName(id);
}


bool MultipleAccessStorage::canInsertImpl(const AccessEntityPtr & entity) const
{
    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        if (storage->canInsert(entity))
            return true;
    }
    return false;
}


UUID MultipleAccessStorage::insertImpl(const AccessEntityPtr & entity, bool replace_if_exists)
{
    std::shared_ptr<IAccessStorage> storage_for_insertion;
    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        if (storage->canInsert(entity))
        {
            storage_for_insertion = storage;
            break;
        }
    }

    if (!storage_for_insertion)
        throw Exception("Not found a storage to insert " + entity->outputTypeAndName(), ErrorCodes::ACCESS_STORAGE_FOR_INSERTION_NOT_FOUND);

    auto id = replace_if_exists ? storage_for_insertion->insertOrReplace(entity) : storage_for_insertion->insert(entity);
    std::lock_guard lock{mutex};
    ids_cache.set(id, storage_for_insertion);
    return id;
}


void MultipleAccessStorage::removeImpl(const UUID & id)
{
    getStorage(id)->remove(id);
}


void MultipleAccessStorage::updateImpl(const UUID & id, const UpdateFunc & update_func)
{
    getStorage(id)->update(id, update_func);
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const
{
    auto storage = findStorage(id);
    if (!storage)
        return {};
    return storage->subscribeForChanges(id, handler);
}


bool MultipleAccessStorage::hasSubscriptionImpl(const UUID & id) const
{
    auto storages = getStorages();
    for (const auto & storage : *storages)
    {
        if (storage->hasSubscription(id))
            return true;
    }
    return false;
}


ext::scope_guard MultipleAccessStorage::subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const
{
    std::unique_lock lock{mutex};
    auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    handlers.push_back(handler);
    auto handler_it = std::prev(handlers.end());
    if (handlers.size() == 1)
        updateSubscriptionsToStorages(lock);

    return [this, type, handler_it]
    {
        std::unique_lock lock2{mutex};
        auto & handlers2 = handlers_by_type[static_cast<size_t>(type)];
        handlers2.erase(handler_it);
        if (handlers2.empty())
            updateSubscriptionsToStorages(lock2);
    };
}


bool MultipleAccessStorage::hasSubscriptionImpl(EntityType type) const
{
    std::lock_guard lock{mutex};
    const auto & handlers = handlers_by_type[static_cast<size_t>(type)];
    return !handlers.empty();
}


/// Updates subscriptions to nested storages.
/// We need the subscriptions to the nested storages if someone has subscribed to us.
/// If any of the nested storages is changed we call our subscribers.
void MultipleAccessStorage::updateSubscriptionsToStorages(std::unique_lock<std::mutex> & lock) const
{
    /// lock is already locked.

    std::vector<std::pair<StoragePtr, ext::scope_guard>> added_subscriptions[static_cast<size_t>(EntityType::MAX)];
    std::vector<ext::scope_guard> removed_subscriptions;

    for (auto type : ext::range(EntityType::MAX))
    {
        auto & handlers = handlers_by_type[static_cast<size_t>(type)];
        auto & subscriptions = subscriptions_to_nested_storages[static_cast<size_t>(type)];
        if (handlers.empty())
        {
            /// None has subscribed to us, we need no subscriptions to the nested storages.
            for (auto & subscription : subscriptions | boost::adaptors::map_values)
                removed_subscriptions.push_back(std::move(subscription));
            subscriptions.clear();
        }
        else
        {
            /// Someone has subscribed to us, now we need to have a subscription to each nested storage.
            for (auto it = subscriptions.begin(); it != subscriptions.end();)
            {
                const auto & storage = it->first;
                auto & subscription = it->second;
                if (boost::range::find(*nested_storages, storage) == nested_storages->end())
                {
                    removed_subscriptions.push_back(std::move(subscription));
                    it = subscriptions.erase(it);
                }
                else
                    ++it;
            }

            for (const auto & storage : *nested_storages)
            {
                if (!subscriptions.count(storage))
                    added_subscriptions[static_cast<size_t>(type)].push_back({storage, nullptr});
            }
        }
    }

    /// Unlock the mutex temporarily because it's much better to subscribe to the nested storages
    /// with the mutex unlocked.
    lock.unlock();
    removed_subscriptions.clear();

    for (auto type : ext::range(EntityType::MAX))
    {
        if (!added_subscriptions[static_cast<size_t>(type)].empty())
        {
            auto on_changed = [this, type](const UUID & id, const AccessEntityPtr & entity)
            {
                Notifications notifications;
                SCOPE_EXIT({ notify(notifications); });
                std::lock_guard lock2{mutex};
                for (const auto & handler : handlers_by_type[static_cast<size_t>(type)])
                    notifications.push_back({handler, id, entity});
            };
            for (auto & [storage, subscription] : added_subscriptions[static_cast<size_t>(type)])
                subscription = storage->subscribeForChanges(type, on_changed);
        }
    }

    /// Lock the mutex again to store added subscriptions to the nested storages.
    lock.lock();
    for (auto type : ext::range(EntityType::MAX))
    {
        if (!added_subscriptions[static_cast<size_t>(type)].empty())
        {
            auto & subscriptions = subscriptions_to_nested_storages[static_cast<size_t>(type)];
            for (auto & [storage, subscription] : added_subscriptions[static_cast<size_t>(type)])
            {
                if (!subscriptions.count(storage) && (boost::range::find(*nested_storages, storage) != nested_storages->end())
                    && !handlers_by_type[static_cast<size_t>(type)].empty())
                {
                    subscriptions.emplace(std::move(storage), std::move(subscription));
                }
            }
        }
    }

    lock.unlock();
    added_subscriptions->clear();
}

}
