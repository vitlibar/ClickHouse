#include <Access/AccessEntitiesVisibility.h>
#include <Access/AccessControlManager.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Access/RowPolicy.h>
#include <Access/Quota.h>
#include <Access/QuotaUsage.h>
#include <Access/SettingsProfile.h>
#include <boost/range/algorithm_ext/erase.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_ENTITY_NOT_FOUND;
}


namespace
{
    using EntityType = IAccessEntity::Type;
    using EntityTypeInfo = IAccessEntity::TypeInfo;


    struct EntityInfo
    {
        EntityInfo(const UUID & id_) : id(id_) {}
        const UUID & id;
        AccessEntityPtr entity;
        std::optional<EntityType> entity_type;
    };


    class GrantsChecker
    {
    public:
        GrantsChecker(const ContextAccess & access_) : access(access_) {}

        bool isVisible(EntityInfo & entity_info) const
        {
            if (show_all())
                return true;

            const auto & id = entity_info.id;
            auto & entity = entity_info.entity;
            auto & entity_type = entity_info.entity_type;

            if (!entity_type)
            {
                if (!entity)
                {
                    entity = access.getManager().tryRead(id);
                    if (!entity)
                        return false;
                }
               entity_type = entity->getType();
            }

            switch (*entity_type)
            {
                case EntityType::USER:
                {
                    if (show_users())
                        return true;
                    return (id == access.getUserID());
                }

                case EntityType::ROLE:
                {
                    if (show_roles())
                        return true;
                    auto roles_info = access.getRolesInfo();
                    return roles_info && roles_info->enabled_roles.count(id);
                }

                case EntityType::ROW_POLICY:
                {
                    if (show_tables())
                        return true;
                    if (!entity)
                    {
                        entity = access.getManager().tryRead(id);
                        if (!entity)
                            return false;
                    }
                    auto policy = typeid_cast<RowPolicyPtr>(entity);
                    return access.isGranted(AccessType::SHOW_TABLES, policy->getDatabase(), policy->getTableName());
                }

                case EntityType::QUOTA:
                {
                    if (show_quotas())
                        return true;
                    auto quota_usage = access.getQuotaUsage();
                    return quota_usage && (id == quota_usage->quota_id);
                }

                case EntityType::SETTINGS_PROFILE:
                {
                    if (show_settings_protiles())
                        return true;
                    auto enabled_profile_ids = access.getEnabledProfileIDs();
                    return enabled_profile_ids && enabled_profile_ids->count(id);
                }

                case EntityType::MAX: break;
            }
            __builtin_unreachable();
        }

        bool isVisible(const UUID & id) const
        {
            EntityInfo entity_info{id};
            return isVisible(entity_info);
        }

        bool isVisible(const UUID & id, EntityType entity_type) const
        {
            EntityInfo entity_info{id};
            entity_info.entity_type = entity_type;
            return isVisible(entity_info);
        }


        std::vector<UUID> findAllVisible(EntityType entity_type) const
        {
            switch (entity_type)
            {
                case EntityType::USER:
                {
                    if (show_users())
                        return access.getManager().findAll<User>();
                    if (auto id = access.getUserID())
                        return {*id};
                    return {};
                }

                case EntityType::ROLE:
                {
                    if (show_roles())
                        return access.getManager().findAll<Role>();
                    if (auto roles_info = access.getRolesInfo())
                        return std::vector<UUID>{roles_info->enabled_roles.begin(), roles_info->enabled_roles.end()};
                    return {};
                }

                case EntityType::ROW_POLICY:
                {
                    auto ids = access.getManager().findAll<RowPolicy>();
                    if (show_tables())
                        return ids;
                    boost::range::remove_erase_if(
                        ids, [&](const UUID & id) -> bool
                    {
                        RowPolicyPtr policy = access.getManager().tryRead<RowPolicy>(id);
                        return !policy || !access.isGranted(AccessType::SHOW_TABLES, policy->getDatabase(), policy->getTableName());
                    });
                    return ids;
                }

                case EntityType::QUOTA:
                {
                    if (show_quotas())
                        return access.getManager().findAll<Quota>();
                    if (auto quota_usage = access.getQuotaUsage())
                        return {quota_usage->quota_id};
                    return {};
                }

                case EntityType::SETTINGS_PROFILE:
                {
                    if (show_settings_protiles())
                        return access.getManager().findAll<SettingsProfile>();
                    if (auto enabled_profile_ids = access.getEnabledProfileIDs())
                        return std::vector<UUID>(enabled_profile_ids->begin(), enabled_profile_ids->end());
                    return {};
                }

                case EntityType::MAX: break;
            }
            __builtin_unreachable();
        }

    private:
        bool show_users() const
        {
            if (!show_users_value)
                show_users_value = access.isGranted(AccessType::SHOW_USERS);
            return *show_users_value;
        }

        bool show_roles() const
        {
            if (!show_roles_value)
                show_roles_value = access.isGranted(AccessType::SHOW_ROLES);
            return *show_roles_value;
        }

        bool show_quotas() const
        {
            if (!show_quotas_value)
                show_quotas_value = access.isGranted(AccessType::SHOW_QUOTAS);
            return *show_quotas_value;
        }

        bool show_settings_protiles() const
        {
            if (!show_settings_profiles_value)
                show_settings_profiles_value = access.isGranted(AccessType::SHOW_SETTINGS_PROFILES);
            return *show_settings_profiles_value;
        }

        bool show_tables() const
        {
            if (!show_tables_value)
                show_tables_value = access.isGranted(AccessType::SHOW_TABLES);
            return *show_tables_value;
        }

        bool show_all() const
        {
            if (!show_all_value)
                show_all_value = show_users() && show_roles() && show_quotas() && show_settings_protiles() && show_tables();
            return *show_all_value;
        }

        const ContextAccess & access;
        mutable std::optional<bool> show_users_value;
        mutable std::optional<bool> show_roles_value;
        mutable std::optional<bool> show_quotas_value;
        mutable std::optional<bool> show_settings_profiles_value;
        mutable std::optional<bool> show_tables_value;
        mutable std::optional<bool> show_all_value;
    };
}


bool AccessEntitiesVisibility::isVisible(const UUID & id) const
{
    return GrantsChecker{access}.isVisible(id);
}


std::optional<UUID> AccessEntitiesVisibility::findVisible(EntityType type, const String & name) const
{
    auto id = access.getManager().find(type, name);
    if (!id)
        return {};
    if (!GrantsChecker{access}.isVisible(*id, type))
        return {};
    return id;
}

std::vector<UUID> AccessEntitiesVisibility::findVisible(EntityType type, const Strings & names) const
{
    auto ids = access.getManager().find(type, names);
    GrantsChecker grants{access};
    boost::range::remove_erase_if(ids, [&](const UUID & id) -> bool { return !grants.isVisible(id, type); });
    return ids;
}


std::vector<UUID> AccessEntitiesVisibility::findAllVisible(EntityType type) const
{
    return GrantsChecker{access}.findAllVisible(type);
}


UUID AccessEntitiesVisibility::getIDIfVisible(EntityType type, const String & name) const
{
    auto id = access.getManager().find(type, name);
    if (!id || !GrantsChecker{access}.isVisible(*id, type))
        throwNotFound(type, name);
    return *id;
}


std::vector<UUID> AccessEntitiesVisibility::getIDsIfVisible(EntityType type, const Strings & names) const
{
    std::vector<UUID> ids;
    ids.reserve(names.size());
    GrantsChecker grants{access};
    for (const String & name : names)
    {
        auto id = access.getManager().find(type, name);
        if (!id || !grants.isVisible(*id, type))
            throwNotFound(type, name);
        ids.emplace_back(*id);
    }
    return ids;
}


String AccessEntitiesVisibility::readNameIfVisible(const UUID & id) const
{
    auto name = tryReadNameIfVisible(id);
    if (name)
        return *name;
    throwNotFound(id);
}

std::optional<String> AccessEntitiesVisibility::tryReadNameIfVisible(const UUID & id) const
{
    auto entity = tryReadIfVisibleImpl(id);
    if (entity)
        return entity->getName();
    return {};
}


AccessEntityPtr AccessEntitiesVisibility::tryReadIfVisibleImpl(const UUID & id) const
{
    EntityInfo entity_info{id};
    if (!GrantsChecker{access}.isVisible(entity_info))
        return nullptr;
    auto & entity = entity_info.entity;
    if (!entity)
        entity = access.getManager().tryRead(id);
    return entity;
}

AccessEntityPtr AccessEntitiesVisibility::tryReadIfVisibleImpl(const UUID & id, EntityType entity_type) const
{
    EntityInfo entity_info{id};
    entity_info.entity_type = entity_type;
    if (!GrantsChecker{access}.isVisible(entity_info))
        return nullptr;
    auto & entity = entity_info.entity;
    if (!entity)
        entity = access.getManager().tryRead(id);
    if (!entity->isTypeOf(entity_type))
        return nullptr;
    return entity;
}

AccessEntityPtr AccessEntitiesVisibility::tryReadIfVisibleImpl(EntityType entity_type, const String & name) const
{
    auto id = access.getManager().find(entity_type, name);
    if (!id)
        return {};
    return tryReadIfVisibleImpl(*id, entity_type);
}

void IAccessStorage::throwNotFound(const UUID & id) const
{
    throw Exception("ID(" + toString(id) + "): not found", ErrorCodes::ACCESS_ENTITY_NOT_FOUND);
}


void IAccessStorage::throwNotFound(EntityType entity_type, const String & name) const
{
    const auto & type_info = EntityTypeInfo::get(entity_type);
    throw Exception("There is no " + type_info.outputWithEntityName(name), type_info.not_found_error_code);
}


const AccessControlManager & AccessEntitiesVisibility::getManager() const
{
    return access.getManager();
}

}
