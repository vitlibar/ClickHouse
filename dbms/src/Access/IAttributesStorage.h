#pragma once

#include <Access/IAttributes.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <functional>
#include <optional>
#include <vector>
#include <atomic>


namespace Poco { class Logger; }

namespace DB
{
/// Contains attributes, i.e. instances of classes derived from IAttributes.
/// The implementations of this class MUST be thread-safe.
class IAttributesStorage
{
public:
    using Type = IAttributes::Type;

    IAttributesStorage(const String & storage_name_) : storage_name(storage_name_) {}
    virtual ~IAttributesStorage() {}

    /// Returns the name of this storage.
    const String & getStorageName() const { return storage_name; }

    /// Returns the identifiers of all the attributes of a specified type contained in the storage.
    std::vector<UUID> findAll(const Type & type) const;

    template <typename AttributesT>
    std::vector<UUID> findAll() const { return findAll(AttributesT::TYPE); }

    /// Returns the identifiers of the attributes which names have a specified prefix.
    std::vector<UUID> findPrefixed(const Type & type, const String & prefix) const;

    template <typename AttributesT>
    std::vector<UUID> findPrefixed(const String & prefix) const { return findPrefixed(AttributesT::TYPE, prefix); }

    /// Searchs for attributes with specified type and name. Returns std::nullopt if not found.
    std::optional<UUID> find(const Type & type, const String & name) const;

    template <typename AttributesT>
    std::optional<UUID> find(const String & name) const { return find(AttributesT::TYPE, name); }

    std::vector<UUID> find(const Type & type, const Strings & names) const;

    template <typename AttributesT>
    std::vector<UUID> find(const Strings & names) const { return find(AttributesT::TYPE, names); }

    /// Searchs for attributes with specified name and type. Throws an exception if not found.
    UUID getID(const Type & type, const String & name) const;

    template <typename AttributesT>
    UUID getID(const String & name) const { return getID(AttributesT::TYPE, name); }

    std::vector<UUID> getIDs(const Type & type, const String & name) const;

    template <typename AttributesT>
    std::vector<UUID> getIDs(const Strings & name) const { return getIDs(AttributesT::TYPE, name); }

    /// Returns whether there are attributes with such identifier in the storage.
    bool exists(const UUID & id) const;

    /// Reads the attributes. Throws an exception if not found.
    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> read(const UUID & id) const;

    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> read(const String & name) const;

    /// Reads the attributes. Returns nullptr if not found.
    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> tryRead(const UUID & id) const;

    template <typename AttributesT = IAttributes>
    std::shared_ptr<const AttributesT> tryRead(const String & name) const;

    /// Reads only name and type of the attributes.
    String readName(const UUID & id) const;
    std::optional<String> tryReadName(const UUID & id) const;

    using OnChangedHandler = std::function<void(const UUID & id, const AttributesPtr & old_attrs, const AttributesPtr & new_attrs);
    using Notification = std::tuple<OnChangedHandler, UUID, AttributesPtr, AttributesPtr>;
    using Notifications = std::vector<Notification>;

    /// Inserts attributes to the storage. Returns IDs of inserted entries.
    /// Throws an exception if the specified name already exists.
    /// If `notifications` is specified, the function will append `notifications` with new notifications instead of sending them.
    UUID insert(const AttributesPtr & attrs, Notifications * notifications = nullptr);
    std::vector<UUID> insert(const std::vector<AttributesPtr> & multiple_attrs, Notifications * notifications = nullptr);

    /// Inserts attributes to the storage. Returns IDs of inserted entries.
    /// Replaces an existing entry in the storage if the specified name already exists.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    UUID insertOrReplace(const AttributesPtr & attrs, Notifications * notifications = nullptr);
    std::vector<UUID> insertOrReplace(const std::vector<AttributesPtr> & multiple_attrs, Notifications * notifications = nullptr);

    /// Inserts attributes to the storage. Returns IDs of inserted entries.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    std::optional<UUID> tryInsert(const AttributesPtr & attrs, Notifications * notifications = nullptr);
    std::vector<UUID> tryInsert(const std::vector<AttributesPtr> & multiple_attrs, Notifications * notifications = nullptr);

    /// Removes an entry from the storage. Throws an exception if couldn't remove.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    void remove(const UUID & id, Notifications * notifications = nullptr);
    void remove(const std::vector<UUID> & ids, Notifications * notifications = nullptr);

    /// Removes an entry from the storage. Returns false if couldn't remove.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    bool tryRemove(const UUID & id, Notifications * notifications = nullptr);

    /// Removes multiple entries from the storage. Returns the list of successfully dropped.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    std::vector<UUID> tryRemove(const std::vector<UUID> & ids, Notifications * notifications = nullptr);

    using UpdateFunc = std::function<void(const UUID &, IAttributes &)>;

    /// Updates the attributes in the storage. Throws an exception if couldn't remove.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    void update(const UUID & id, const UpdateFunc & update_func, Notifications * notifications = nullptr);
    void update(const std::vector<UUID> & ids, const UpdateFunc & update_func, Notifications * notifications = nullptr);

    /// Updates an entry in the storage. Returns false if couldn't update.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    bool tryUpdate(const UUID & id, const UpdateFunc & update_func, Notifications * notifications = nullptr);

    /// Updates multiple entries in the storage. Returns the list of successfully updated.
    /// If `notifications` is not null, the function will append `notifications` with new notifications instead of sending them.
    std::vector<UUID> tryUpdate(const std::vector<UUID> & ids, const UpdateFunc & update_func, Notifications * notifications = nullptr);

    class Subscription
    {
    public:
        virtual ~Subscription() {}
    };

    using SubscriptionPtr = std::unique_ptr<Subscription>;

    /// Subscribes for all changes.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    SubscriptionPtr subscribeForChangesOfAll(const Type & type, const OnChangedHandler & handler);

    template <typename AttributesT>
    SubscriptionPtr subscribeForChangesOfAll(OnChangedHandler handler) { return subscribeForChangesOfAll(AttributesT::TYPE, handler); }

    SubscriptionPtr subscribeForChangesOfPrefixed(const Type & type, const String & prefix, const OnChangedHandler & handler);

    template <typename AttributesT>
    SubscriptionPtr subscribeForChangesOfPrefixed(const String & prefix, const OnChangedHandler & handler) { return subscribeForChangesOfPrefixed(AttributesT::TYPE, prefix, handler); }

    /// Subscribes for changes of a specific entry.
    /// Can return nullptr if cannot subscribe (identifier not found) or if it doesn't make sense (the storage is read-only).
    SubscriptionPtr subscribeForChanges(const UUID & id, const OnChangedHandler & handler);
    SubscriptionPtr subscribeForChanges(const std::vector<UUID> & ids, const OnChangedHandler & handler);

    static void notify(const Notifications & notifications);

protected:
    virtual std::vector<UUID> findPrefixedImpl(const Type & type, const String & prefix) const = 0;
    virtual std::optional<UUID> findImpl(const Type & type, const String & name) const = 0;
    virtual bool existsImpl(const UUID & id) const = 0;
    virtual AttributesPtr readImpl(const UUID & id) const = 0;
    virtual String readNameImpl(const UUID & id) const = 0;
    virtual UUID insertImpl(const AttributesPtr & attrs, bool replace_if_exists, Notifications & notifications) = 0;
    virtual void removeImpl(const UUID & id, Notifications & notifications) = 0;
    virtual void updateImpl(const UUID & id, const UpdateFunc & update_func, Notifications & notifications) = 0;
    virtual SubscriptionPtr subscribeForChangesOfPrefixedImpl(const Type & type, const String & prefix, const OnChangedHandler & handler) const = 0;
    virtual SubscriptionPtr subscribeForChangesImpl(const UUID & id, const OnChangedHandler & handler) const = 0;

    static UUID generateRandomID();
    UUID generateIDFromNameAndType(const String & name, const Type & type) const;

    Poco::Logger * getLogger() const;
    [[noreturn]] void throwNotFound(const UUID & id) const;
    [[noreturn]] void throwNotFound(const String & name, const Type & type) const;
    [[noreturn]] void throwNameCollisionCannotInsert(const String & name, const Type & type, const Type & type_of_existing) const;
    [[noreturn]] void throwNameCollisionCannotRename(const String & old_name, const String & new_name, const Type & type, const Type & type_of_existing) const;
    [[noreturn]] void throwStorageIsReadOnly() const;

private:
    AttributesPtr tryReadImpl(const UUID & id) const;

    const String storage_name;
    mutable std::atomic<Poco::Logger *> log = nullptr;
};


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::read(const UUID & id) const
{
    auto attrs = readImpl(id);
    if (!attrs)
        return nullptr;
    if constexpr (std::is_same_v<AttributesT, IAttributes>)
        return attrs;
    else
        return attrs->cast<AttributesT>();
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::read(const String & name) const
{
    return read<AttributesT>(getID(name, AttributesT::TYPE));
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::tryRead(const UUID & id) const
{
    auto attrs = tryReadImpl(id);
    if (!attrs)
        return nullptr;
    if constexpr (std::is_same_v<AttributesT, IAttributes>)
        return attrs;
    else
        return attrs->tryCast<AttributesT>();
}


template <typename AttributesT>
std::shared_ptr<const AttributesT> IAttributesStorage::tryRead(const String & name) const
{
    auto id = find(name, AttributesT::TYPE);
    return id ? tryRead<AttributesT>(*id) : nullptr;
}
}
