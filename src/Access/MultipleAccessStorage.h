#pragma once

#include <Access/IAccessStorage.h>
#include <Common/LRUCache.h>
#include <mutex>


namespace DB
{
/// Implementation of IAccessStorage which contains multiple nested storages.
class MultipleAccessStorage : public IAccessStorage
{
public:
    using Storage = IAccessStorage;
    using StoragePtr = std::shared_ptr<Storage>;
    using StorageConstPtr = std::shared_ptr<const Storage>;

    MultipleAccessStorage(const String & storage_name_ = "multiple");

    size_t getNumberOfStorages() const;
    StoragePtr getStorageByIndex(size_t i);
    StorageConstPtr getStorageByIndex(size_t i) const;
    void setStorages(const std::vector<StoragePtr> & storages);
    void addStorage(const StoragePtr & new_storage, size_t i = static_cast<size_t>(-1));
    void removeStorage(size_t i);

    StorageConstPtr findStorage(const UUID & id) const;
    StoragePtr findStorage(const UUID & id);
    StorageConstPtr getStorage(const UUID & id) const;
    StoragePtr getStorage(const UUID & id);

protected:
    std::optional<UUID> findImpl(EntityType type, const String & name) const override;
    std::vector<UUID> findAllImpl(EntityType type) const override;
    bool existsImpl(const UUID & id) const override;
    AccessEntityPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID &id) const override;
    bool canInsertImpl(const AccessEntityPtr & entity) const override;
    UUID insertImpl(const AccessEntityPtr & entity, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    ext::scope_guard subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const override;
    ext::scope_guard subscribeForChangesImpl(EntityType type, const OnChangedHandler & handler) const override;
    bool hasSubscriptionImpl(const UUID & id) const override;
    bool hasSubscriptionImpl(EntityType type) const override;

private:
    using Storages = std::vector<StoragePtr>;
    std::shared_ptr<const Storages> getStorages() const;
    void updateSubscriptionsToStorages(std::unique_lock<std::mutex> & lock) const;

    std::shared_ptr<const Storages> nested_storages;
    mutable LRUCache<UUID, Storage> ids_cache;
    mutable std::list<OnChangedHandler> handlers_by_type[static_cast<size_t>(EntityType::MAX)];
    mutable std::unordered_map<StoragePtr, ext::scope_guard> subscriptions_to_nested_storages[static_cast<size_t>(EntityType::MAX)];
    mutable std::mutex mutex;
};

}
