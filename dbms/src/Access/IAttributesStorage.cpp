#include <Access/IAttributesStorage.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/MD5Engine.h>
#include <Poco/Logger.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTRIBUTES_NOT_FOUND;
    extern const int ATTRIBUTES_NOT_INSERTED;
    extern const int ATTRIBUTES_NOT_REMOVED;
    extern const int ATTRIBUTES_NOT_UPDATED;
    extern const int ATTRIBUTES_READ_ONLY;
}


std::vector<UUID> IAttributesStorage::findAll(const Type & type) const
{
    return findPrefixedImpl(type, "");
}


std::vector<UUID> IAttributesStorage::findPrefixed(const Type & type, const String & prefix) const
{
    return findPrefixedImpl(type, prefix);
}


std::optional<UUID> IAttributesStorage::find(const Type & type, const String & name) const
{
    return findImpl(type, name);
}


std::vector<UUID> IAttributesStorage::find(const Type & type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
        ids.push_back(findImpl(type, name));
    return ids;
}


UUID IAttributesStorage::getID(const Type & type, const String & name) const
{
    auto id = find(name, type);
    if (id)
        return *id;
    throwNotFound(name, type);
}


std::vector<UUID> IAttributesStorage::getIDs(const Type & type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    for (const String & name : names)
        ids.push_back(getID(type, name));
    return ids;
}


bool IAttributesStorage::exists(const UUID & id) const
{
    return existsImpl(id);
}



AttributesPtr IAttributesStorage::tryReadImpl(const UUID & id) const
{
    try
    {
        return readImpl(id);
    }
    catch (...)
    {
        return nullptr;
    }
}


String IAttributesStorage::readName(const UUID & id) const
{
    return readNameImpl(id);
}


std::optional<String> IAttributesStorage::tryReadName(const UUID & id) const
{
    try
    {
        return readNameImpl(id);
    }
    catch (...)
    {
        return {};
    }
}


UUID IAttributesStorage::insert(const AttributesPtr & attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    UUID id = insertImpl(attrs, false, *notifications);
    notify(immediate_notifications);
    return id;
}


std::vector<UUID> IAttributesStorage::insert(const std::vector<AttributesPtr> & multiple_attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::vector<UUID> ids;
    ids.reserve(multiple_attrs.size());
    std::exception_ptr e;
    for (const auto & attrs : multiple_attrs)
    {
        UUID id;
        try
        {
            id = insertImpl(attrs, false, *notifications);
        }
        catch (...)
        {
            if (!e)
                e = std::current_exception();
            continue;
        }
        ids.push_back(id);
    }

    if (e)
    {
        std::rethrow_exception(e);
    }

    notify(immediate_notifications);
    return ids;
}


UUID IAttributesStorage::insertOrReplace(const AttributesPtr & attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    UUID id = insertImpl(attrs, true, *notifications);
    notify(immediate_notifications);
    return id;
}


std::vector<UUID> IAttributesStorage::insertOrReplace(const std::vector<AttributesPtr> & multiple_attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::vector<UUID> ids;
    ids.reserve(multiple_attrs.size());
    std::exception_ptr e;
    for (const auto & attrs : multiple_attrs)
    {
        UUID id;
        try
        {
            id = insertImpl(attrs, true, *notifications);
        }
        catch (...)
        {
            if (!e)
                e = std::current_exception();
            continue;
        }
        ids.push_back(id);
    }

    if (e)
        std::rethrow_exception(e);

    notify(immediate_notifications);
    return ids;
}


std::optional<UUID> IAttributesStorage::tryInsert(const AttributesPtr & attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    UUID id;
    try
    {
        id = insertImpl(attrs, false, *notifications);
    }
    catch (...)
    {
        return std::nullopt;
    }
    notify(immediate_notifications);
    return id;
}


std::vector<UUID> IAttributesStorage::tryInsert(const std::vector<AttributesPtr> & multiple_attrs, Notification * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::vector<UUID> ids;
    ids.reserve(multiple_attrs.size());
    for (const auto & attrs : multiple_attrs)
    {
        UUID id;
        try
        {
            id = insertImpl(attrs, false, *notifications);
        }
        catch (...)
        {
            continue;
        }
        ids.push_back(id);
    }
    notify(immediate_notifications);
    return ids;
}


void IAttributesStorage::remove(const UUID & id, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    removeImpl(id, *notifications);
    notify(immediate_notifications);
}


void IAttributesStorage::remove(const std::vector<UUID> & ids, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::exception_ptr e;
    for (const auto & id : ids)
    {
        try
        {
            removeImpl(id, *notifications);
        }
        catch (...)
        {
            if (!e)
                e = std::current_exception();
            continue;
        }
    }

    if (e)
        std::rethrow_exception(e);

    notify(immediate_notifications);
}


bool IAttributesStorage::tryRemove(const UUID & id, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    try
    {
        removeImpl(id, *notifications);
    }
    catch (...)
    {
        return false;
    }

    notify(immediate_notifications);
    return true;
}


std::vector<UUID> IAttributesStorage::tryRemove(const std::vector<UUID> & ids, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::vector<UUID> removed;
    removed.reserve(ids.size());
    for (const auto & id : ids)
    {
        try
        {
            removeImpl(id, *notifications);
        }
        catch (...)
        {
            continue;
        }
        removed.push_back(id);
    }

    notify(immediate_notifications);
    return removed;
}


void IAttributesStorage::update(const UUID & id, const UpdateFunc & update_func, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    updateImpl(id, update_func, *notifications);
    notify(immediate_notifications);
}


void IAttributesStorage::update(const std::vector<UUID> & ids, const UpdateFunc & update_func, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::exception_ptr e;
    for (const auto & id : ids)
    {
        try
        {
            updateImpl(id, update_func, *notifications);
        }
        catch (...)
        {
            if (!e)
                e = std::current_exception();
            continue;
        }
    }

    if (e)
        std::rethrow_exception(e);

    notify(immediate_notifications);
}


bool IAttributesStorage::tryUpdate(const UUID & id, const UpdateFunc & update_func, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    try
    {
        updateImpl(id, update_func, *notifications);
    }
    catch (...)
    {
        return false;
    }

    notify(immediate_notifications);
    return true;
}


std::vector<UUID> IAttributesStorage::tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func, Notifications * notifications)
{
    Notifications immediate_notifications;
    if (!notifications)
        notifications = &immediate_notifications;
    std::vector<UUID> updated;
    updated.reserve(ids.size());
    for (const auto & id : ids)
    {
        try
        {
            updateImpl(id, update_func, *notifications);
        }
        catch (...)
        {
            continue;
        }
        updated.push_back(id);
    }

    notify(immediate_notifications);
    return updated;
}


IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChangesOfAll(const Type & type, const OnChangedHandler & handler)
{
    return subscribeForChangesOfPrefixedImpl(type, "", handler);
}


IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChangesOfPrefixed(const Type & type, const String & prefix, const OnChangedHandler & handler)
{
    return subscribeForChangesOfPrefixedImpl(type, prefix, handler);
}


IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChanges(const UUID & id, const OnChangedHandler & handler)
{
    return subscribeForChangesImpl(if, handler);
}


IAttributesStorage::SubscriptionPtr IAttributesStorage::subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler)
{
    if (ids.empty())
        return nullptr;
    if (ids.size() == 1)
        return subscribeForChangesImpl(ids[0], handler);

    std::vector<SubscriptionPtr> subscriptions;
    subscriptions.reserve(ids.size());
    for (const auto & id : ids)
    {
        auto subscription = subscribeForChangesImpl(id, handler);
        if (subscription)
            subscriptions.push_back(std::move(subscription));
    }

    struct MultiSubscription : public Subscription
    {
        std::vector<SubscriptionPtr> subscriptions;
    };

    return std::make_unique<MultiSubscription>{std::move(subscriptions)};
}


void IAttributesStorage::notify(const Notifications & notifications)
{
    for (const auto & [fn, id, old_attrs, new_attrs] : notifications)
        fn(id, old_attrs, new_attrs);
}


UUID IAttributesStorage::generateRandomID()
{
    static Poco::UUIDGenerator generator;
    UUID id;
    generator.createRandom().copyTo(reinterpret_cast<char *>(&id));
    return id;
}


UUID IAttributesStorage::generateIDFromNameAndType(const String & name, const Type & type) const
{
    Poco::MD5Engine md5;
    md5.update(storage_name);
    md5.update(type.name, strlen(type.name));
    md5.update(name);
    UUID result;
    memcpy(&result, md5.digest().data(), md5.digestLength());
    return result;
}


Poco::Logger * IAttributesStorage::getLogger() const
{
    Poco::Logger * ptr = log.load();
    if (!ptr)
        log.store(ptr = &Poco::Logger::get("AttributesStorage(" + storage_name + ")"), std::memory_order_relaxed);
    return ptr;
}


void IAttributesStorage::throwNotFound(const UUID & id) const
{
    throw Exception("ID {" + toString(id) + "} not found in AttributesStorage(" + getStorageName() + ")", ErrorCodes::ATTRIBUTES_NOT_FOUND);
}


void IAttributesStorage::throwNotFound(const String & name, const Type & type) const
{
    throw Exception(String(type.name) + " " + backQuote(name) + " not found in AttributesStorage(" + getStorageName() + ")", ErrorCodes::ATTRIBUTES_NOT_FOUND);
}


void IAttributesStorage::throwNameCollisionCannotInsert(const String & name, const Type & type, const Type & type_of_existing) const
{
    throw Exception(
        String(type.name) + " " + backQuote(name) + ": cannot insert because " + type_of_existing.name + " " + backQuote(name)
            + " already exists in AttributesStorage(" + getStorageName() + ")",
        ErrorCodes::ATTRIBUTES_NOT_INSERTED);
}


void IAttributesStorage::throwNameCollisionCannotRename(const String & old_name, const String & new_name, const Type & type, const Type & type_of_existing) const
{
    throw Exception(
        String(type.name) + " " + backQuote(old_name) + ": cannot rename to " + backQuote(new_name) + " because " + type_of_existing.name
            + " " + backQuote(new_name) + " already exists in AttributesStorage(" + getStorageName() + ")",
        ErrorCodes::ATTRIBUTES_NOT_UPDATED);
}


void IAttributesStorage::throwStorageIsReadOnly() const
{
    throw Exception("AttributesStorage(" + getStorageName() + ") is readonly", ErrorCodes::ATTRIBUTES_READ_ONLY);
}
}
