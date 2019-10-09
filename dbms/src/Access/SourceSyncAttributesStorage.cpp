#include <Access/SourceSyncAttributesStorage.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>


namespace DB
{
SourceSyncAttributesStorage::SourceSyncAttributesStorage(const String & storage_name_)
    : IAttributesStorage(storage_name_)
{
}

SourceSyncAttributesStorage::~SourceSyncAttributesStorage() = default;


void SourceSyncAttributesStorage::updateFromSource()
{
    NamesAndTypes names_and_types;
    try
    {
        names_and_types = readNamesAndTypesFromSource();
    }
    catch (...)
    {
        tryLogCurrentException(getLogger(), "Couldn't get list of stored attributes in AttributesStorage(" + getStorageName() + ")");
        return;
    }

    /// Put the new attributes into helper map and check for name collision.
    struct Hash
    {
        size_t operator()(const std::pair<StringRef, size_t> & x) const { return StringRefHash{}(x.first) + x.second; }
    };
    std::unordered_map<std::pair<StringRef, size_t>, const Type *, Hash> in_use;

    for (auto it = names_and_types.begin(); it != names_and_types.end();)
    {
        const String & name = it->first;
        const Type & type = *(it->second);
        if (in_use.find({name, type.namespace_idx}) != in_use.end())
        {
            LOG_WARNING(
                getLogger(), String(type.name) + " " + backQuote(name) + ": duplication found in AttributesStorage(" + getStorageName() + "), only the first encounter will be used");
            it = names_and_types.erase(it);
        }
        else
        {
            in_use[std::pair{name, type.namespace_idx}] = &type;
            ++it;
        }
    }

    OnChangeNotifications on_change_notifications;
    OnNewNotifications on_new_notifications;

    std::unique_lock lock{mutex};

    /// Removing the attributes which are not listed in `all_attr2`.
    for (auto it = entries.begin(); it != entries.end();)
    {
        Entry & entry = it->second;
        const String & name = entry.name;
        const Type & type = *entry.type;
        auto in_use_pos = in_use.find(std::pair{name, type.namespace_idx});
        if ((in_use_pos != in_use.end()) && (in_use_pos->second == &type))
            ++it;
        else
        {
            /// Prepare list of notifications.
            for (auto handler : entry.on_changed_handlers)
                on_change_notifications.emplace_back(handler, nullptr);

            /// Do removing.
            all_names[type.namespace_idx].erase(name);
            it = entries.erase(it);
        }
    }

    for (const auto & [name, type] : names_and_types)
    {
        size_t namespace_idx = type->namespace_idx;
        if (namespace_idx >= all_names.size())
            all_names.resize(namespace_idx + 1);
        auto & names = all_names[namespace_idx];
        auto it = names.find(name);
        if (it == names.end())
        {
            /// Do insertion.
            UUID id = generateIDFromNameAndType(name, *type);
            names[name] = id;
            Entry & entry = entries[id];
            entry.name = name;
            entry.type = type;

            /// Prepare list of notifications.
            if (namespace_idx < on_new_handlers.size())
            {
                for (auto handler_it = on_new_handlers[namespace_idx].lower_bound(name); handler_it != on_new_handlers[namespace_idx].end();
                     ++handler_it)
                {
                    const String & prefix = handler_it->first;
                    if (!startsWith(name, prefix))
                        break;
                    on_new_notifications.emplace_back(handler_it->second, id);
                }
            }
        }
        else
        {
            Entry & entry = entries[it->second];
            if (entry.on_changed_handlers.empty())
            {
                entry.attrs = nullptr;
            }
            else
            {
                AttributesPtr new_attrs;
                try
                {
                    new_attrs = readFromSource(name, *type);
                }
                catch (...)
                {
                    tryLogCurrentException(getLogger(), "Couldn't update " + String(type->name) + " " + backQuote(name) + " in AttributesStorage(" + getStorageName() + ")");
                }
                if (new_attrs && (!entry.attrs || (*entry.attrs != *new_attrs)))
                {
                    entry.attrs = new_attrs;

                    /// Prepare list of notifications.
                    for (const auto & handler : entry.on_changed_handlers)
                        on_change_notifications.emplace_back(handler, new_attrs);
                }
            }
        }
    }

    /// Notify the subscribers with the `mutex` unlocked.
    lock.unlock();
    notify(on_change_notifications);
    notify(on_new_notifications);
}


std::vector<UUID> SourceSyncAttributesStorage::findPrefixedImpl(const String & prefix, const Type & type) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= all_names.size())
        return {};
    const auto & names = all_names[namespace_idx];
    std::vector<UUID> result;

    if (prefix.empty())
    {
        result.reserve(names.size());
        for (const auto & name_and_id : names)
        {
            const UUID & id = name_and_id.second;
            result.emplace_back(id);
        }
    }
    else
    {
        for (auto it = names.lower_bound(prefix); it != names.end(); ++it)
        {
            const auto & [name, id] = *it;
            if (!startsWith(name, prefix))
                break;
            result.emplace_back(id);
        }
    }
    return result;
}


std::optional<UUID> SourceSyncAttributesStorage::findImpl(const String & name, const Type & type) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= all_names.size())
        return {};

    const auto & names = all_names[namespace_idx];
    auto it = names.find(name);
    if (it == names.end())
        return {};

    return it->second;
}


bool SourceSyncAttributesStorage::existsImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    return entries.count(id);
}


AttributesPtr SourceSyncAttributesStorage::readImpl(const UUID & id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);

    const Entry & entry = it->second;
    if (entry.attrs)
        return entry.attrs;

    entry.attrs = readFromSource(entry.name, *entry.type);
    return entry.attrs;
}


String SourceSyncAttributesStorage::readNameImpl(const UUID &id) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        throwNotFound(id);
    return it->second.name;
}


UUID SourceSyncAttributesStorage::insertImpl(const IAttributes &, bool)
{
    throwStorageIsReadOnly();
}


void SourceSyncAttributesStorage::removeImpl(const UUID &)
{
    throwStorageIsReadOnly();
}


void SourceSyncAttributesStorage::updateImpl(const UUID &, const UpdateFunc &)
{
    throwStorageIsReadOnly();
}


class SourceSyncAttributesStorage::SubscriptionForNew : public IAttributesStorage::Subscription
{
public:
    SubscriptionForNew(
        const SourceSyncAttributesStorage * storage_, size_t namespace_idx_, const std::multimap<String, OnNewHandler>::iterator & handler_it_)
        : storage(storage_), namespace_idx(namespace_idx_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForNew() override { storage->removeSubscription(namespace_idx, handler_it); }

private:
    const SourceSyncAttributesStorage * storage;
    size_t namespace_idx;
    std::multimap<String, OnNewHandler>::iterator handler_it;
};


IAttributesStorage::SubscriptionPtr SourceSyncAttributesStorage::subscribeForNewImpl(const String & prefix, const Type & type, const OnNewHandler & on_new) const
{
    std::lock_guard lock{mutex};
    size_t namespace_idx = type.namespace_idx;
    if (namespace_idx >= on_new_handlers.size())
        on_new_handlers.resize(namespace_idx + 1);
    return std::make_unique<SubscriptionForNew>(this, namespace_idx, on_new_handlers[namespace_idx].emplace(prefix, on_new));
}


void SourceSyncAttributesStorage::removeSubscription(size_t namespace_idx, const std::multimap<String, OnNewHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    if (namespace_idx >= on_new_handlers.size())
        return;
    on_new_handlers[namespace_idx].erase(handler_it);
}


class SourceSyncAttributesStorage::SubscriptionForChanges : public IAttributesStorage::Subscription
{
public:
    SubscriptionForChanges(
        const SourceSyncAttributesStorage * storage_, const UUID & id_, const std::list<OnChangedHandler>::iterator & handler_it_)
        : storage(storage_), id(id_), handler_it(handler_it_)
    {
    }
    ~SubscriptionForChanges() override { storage->removeSubscription(id, handler_it); }

private:
    const SourceSyncAttributesStorage * storage;
    UUID id;
    std::list<OnChangedHandler>::iterator handler_it;
};


IAttributesStorage::SubscriptionPtr SourceSyncAttributesStorage::subscribeForChangesImpl(const UUID & id, const OnChangedHandler & on_changed) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return nullptr;
    const Entry & entry = it->second;
    return std::make_unique<SubscriptionForChanges>(
        this, id, entry.on_changed_handlers.emplace(entry.on_changed_handlers.end(), on_changed));
}


void SourceSyncAttributesStorage::removeSubscription(const UUID & id, const std::list<OnChangedHandler>::iterator & handler_it) const
{
    std::lock_guard lock{mutex};
    auto it = entries.find(id);
    if (it == entries.end())
        return;
    const Entry & entry = it->second;
    entry.on_changed_handlers.erase(handler_it);
}
}
