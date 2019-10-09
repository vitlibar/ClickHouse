#pragma once

#include <Access/MemoryAttributesStorage.h>


namespace DB
{
/// Implementation of IAttributesStorage which get data from some source but never changes this source.
class SourceSyncAttributesStorage : public IAttributesStorage
{
public:
    SourceSyncAttributesStorage(const String & storage_name_);
    ~SourceSyncAttributesStorage() override;

    using NameAndType = std::pair<String, const Type *>;
    using NamesAndTypes = std::vector<NameAndType>;

    /// Reads data from the source and updates the storage.
    void updateFromSource();

protected:
    virtual NamesAndTypes readNamesAndTypesFromSource() const = 0;
    virtual AttributesPtr readFromSource(const String & name, const Type & type) const = 0;

private:
    std::vector<UUID> findPrefixedImpl(const String & prefix, const Type & type) const override;
    std::optional<UUID> findImpl(const String & name, const Type & type) const override;
    bool existsImpl(const UUID & id) const override;
    AttributesPtr readImpl(const UUID & id) const override;
    String readNameImpl(const UUID &id) const override;
    UUID insertImpl(const IAttributes & attrs, bool replace_if_exists) override;
    void removeImpl(const UUID & id) override;
    void updateImpl(const UUID & id, const UpdateFunc & update_func) override;
    SubscriptionPtr subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const override;
    SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const override;

    struct Entry
    {
        String name;
        const Type * type;
        mutable AttributesPtr attrs;
        mutable std::list<OnChangedHandler> on_changed_handlers;
    };

    class SubscriptionForNew;
    class SubscriptionForChanges;
    void removeSubscription(size_t namespace_idx, const std::multimap<String, OnNewHandler>::iterator & handler_it) const;
    void removeSubscription(const UUID & id, const std::list<OnChangedHandler>::iterator & handler_it) const;

    std::vector<std::map<String, UUID>> all_names; /// IDs by namespace_idx and name
    std::unordered_map<UUID, Entry> entries; /// entries by ID
    mutable std::vector<std::multimap<String, OnNewHandler>> on_new_handlers; /// "on_new" handlers by namespace_idx and prefix
    mutable std::mutex mutex;
};
}
