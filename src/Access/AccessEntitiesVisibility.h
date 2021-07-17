#pragma once

#include <Access/IAccessEntity.h>
#include <Core/UUID.h>
#include <memory>
#include <optional>


namespace DB
{
class AccessControlManager;
class ContextAccess;


/// Utility class helps finding access entities which are visible for the current user.
class AccessEntitiesVisibility
{
public:
    using EntityType = IAccessEntity::Type;

    /// Returns true if a specified entity can be seen by the current user.
    bool isVisible(const UUID & id) const;

    /// Searches for an entity with specified type and name.
    /// Returns std::nullopt if not found or cannot be seen by the current user.
    std::optional<UUID> findVisible(EntityType type, const String & name) const;

    template <typename EntityClassT>
    std::optional<UUID> findVisible(const String & name) const { return find(EntityClassT::TYPE, name); }

    std::vector<UUID> findVisible(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> findVisible(const Strings & names) const { return find(EntityClassT::TYPE, names); }

    /// Returns the identifiers of all the entities which can be seen by the current user.
    template <typename EntityClassT>
    std::vector<UUID> findAllVisible() const { return findAllVisible(EntityClassT::TYPE); }

    std::vector<UUID> findAllVisible(IAccessEntity::Type type) const;

    /// Searches for an entity with specified name and type.
    /// Throws an exception if not found or cannot be seen by the current user.
    UUID getIDIfVisible(EntityType type, const String & name) const;

    template <typename EntityClassT>
    UUID getIDIfVisible(const String & name) const { return getID(EntityClassT::TYPE, name); }

    std::vector<UUID> getIDsIfVisible(EntityType type, const Strings & names) const;

    template <typename EntityClassT>
    std::vector<UUID> getIDsIfVisible(const Strings & names) const { return getIDs(EntityClassT::TYPE, names); }

    /// Reads an entity. Throws an exception if not found or cannot be seen by the current user.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> readIfVisible(const UUID & id) const;

    template <typename EntityClassT>
    std::shared_ptr<const EntityClassT> readIfVisible(const String & name) const;

    /// Reads an entity. Returns nullptr if not found or cannot be seen by the current user.
    template <typename EntityClassT = IAccessEntity>
    std::shared_ptr<const EntityClassT> tryReadIfVisible(const UUID & id) const;

    template <typename EntityClassT>
    std::shared_ptr<const EntityClassT> tryReadIfVisible(const String & name) const;

    /// Reads only name of an entity.
    String readNameIfVisible(const UUID & id) const;
    std::optional<String> tryReadNameIfVisible(const UUID & id) const;

    const AccessControlManager & getManager() const;

private:
    friend class ContextAccess;
    AccessEntitiesVisibility(const ContextAccess & access_) : access(access_) {}

    AccessEntityPtr tryReadIfVisibleImpl(const UUID & id) const;
    AccessEntityPtr tryReadIfVisibleImpl(const UUID & id, EntityType entity_type) const;
    AccessEntityPtr tryReadIfVisibleImpl(EntityType entity_type, const String & name) const;
    [[noreturn]] void throwNotFound(const UUID & id) const;
    [[noreturn]] void throwNotFound(EntityType entity_type, const String & name) const;

    const ContextAccess & access;
};


template <typename EntityClassT>
std::shared_ptr<const EntityClassT> AccessEntitiesVisibility::readIfVisible(const UUID & id) const
{
    auto entity = tryReadIfVisible<EntityClassT>(id);
    if (entity)
        return entity;
    throwNotFound(id);
}

template <typename EntityClassT>
std::shared_ptr<const EntityClassT> AccessEntitiesVisibility::tryReadIfVisible(const UUID & id) const
{
    if constexpr (std::is_same_v<EntityClassT, IAccessEntity>)
        return tryReadIfVisibleImpl(id);
    else
        return typeid_cast<std::shared_ptr<const EntityClassT>>(tryReadIfVisibleImpl(id, EntityClassT::TYPE));
}

template <typename EntityClassT>
std::shared_ptr<const EntityClassT> AccessEntitiesVisibility::readIfVisible(const String & name) const
{
    auto entity = tryReadIfVisible<EntityClassT>(name);
    if (entity)
        return entity;
    throwNotFound(EntityClassT::TYPE, name);
}

template <typename EntityClassT>
std::shared_ptr<const EntityClassT> AccessEntitiesVisibility::tryReadIfVisible(const String & name) const
{
    return typeid_cast<std::shared_ptr<const EntityClassT>>(tryReadIfVisibleImpl(EntityClassT::TYPE, name));
}

}
