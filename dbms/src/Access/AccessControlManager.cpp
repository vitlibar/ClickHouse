#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/User.h>
#include <Access/EnabledRolesWatcher.h>
#include <Access/QuotaContextFactory.h>
#include <Access/RowPolicyContextFactory.h>
#include <Access/AccessRightsContext.h>
#include <Parsers/ASTRoleList.h>


namespace DB
{
namespace
{
    std::vector<std::unique_ptr<IAccessStorage>> createStorages()
    {
        std::vector<std::unique_ptr<IAccessStorage>> list;
        list.emplace_back(std::make_unique<MemoryAccessStorage>());
        list.emplace_back(std::make_unique<UsersConfigAccessStorage>());
        return list;
    }
}


AccessControlManager::AccessControlManager()
    : MultipleAccessStorage(createStorages()),
      quota_context_factory(std::make_unique<QuotaContextFactory>(*this)),
      row_policy_context_factory(std::make_unique<RowPolicyContextFactory>(*this))
{
}


AccessControlManager::~AccessControlManager()
{
}


UserPtr AccessControlManager::getUser(
    const String & user_name, std::function<void(const UserPtr &)> on_change, ext::scope_guard * subscription) const
{
    return getUser(getID<User>(user_name), std::move(on_change), subscription);
}


UserPtr AccessControlManager::getUser(
    const UUID & user_id, std::function<void(const UserPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (on_change && subscription)
    {
        *subscription = subscribeForChanges(user_id, [on_change](const UUID &, const AccessEntityPtr & user)
        {
            if (user)
                on_change(typeid_cast<UserPtr>(user));
        });
    }
    return read<User>(user_id);
}


UserPtr AccessControlManager::authorizeAndGetUser(
    const String & user_name,
    const String & password,
    const Poco::Net::IPAddress & address,
    std::function<void(const UserPtr &)> on_change,
    ext::scope_guard * subscription) const
{
    return authorizeAndGetUser(getID<User>(user_name), password, address, std::move(on_change), subscription);
}


UserPtr AccessControlManager::authorizeAndGetUser(
    const UUID & user_id,
    const String & password,
    const Poco::Net::IPAddress & address,
    std::function<void(const UserPtr &)> on_change,
    ext::scope_guard * subscription) const
{
    auto user = getUser(user_id, on_change, subscription);
    user->allowed_client_hosts.checkContains(address, user->getName());
    user->authentication.checkPassword(password, user->getName());
    return user;
}


std::vector<UUID> AccessControlManager::getUsersFromList(const ASTRoleList & list, std::optional<UUID> current_user) const
{
    std::vector<UUID> user_ids;
    if (list.all)
    {
        user_ids = findAll<User>();
        for (const auto & except_name : list.except_names)
        {
            auto except_id = find<User>(except_name);
            if (except_id)
            {
                auto it = boost::range::find(user_ids, except_id);
                if (it != user_ids.end())
                    user_ids.erase(it);
            }
        }
        if (list.except_current_user && current_user)
        {
            auto it = boost::range::find(user_ids, *current_user);
            if (it != user_ids.end())
                user_ids.erase(it);
        }
    }
    else
    {
        for (const auto & name : list.names)
            user_ids.push_back(getID<User>(name));
        if (list.current_user && current_user)
            user_ids.push_back(*current_user);
    }
    return user_ids;
}


std::pair<std::vector<UUID>, std::vector<UUID>> AccessControlManager::getUsersAndRolesFromList(const ASTRoleList & list, std::optional<UUID> current_user) const
{
    std::vector<UUID> user_ids;
    std::vector<UUID> role_ids;
    if (list.all)
    {
        user_ids = findAll<User>();
        role_ids = findAll<Role>();
        for (const auto & except_name : list.except_names)
        {
            auto except_id = find<User>(except_name);
            if (except_id)
            {
                auto it = boost::range::find(user_ids, except_id);
                if (it != user_ids.end())
                    user_ids.erase(it);
            }
            except_id = find<Role>(except_name);
            if (except_id)
            {
                auto it = boost::range::find(role_ids, except_id);
                if (it != role_ids.end())
                    role_ids.erase(it);
            }
        }
        if (list.except_current_user && current_user)
        {
            auto it = boost::range::find(user_ids, *current_user);
            if (it != user_ids.end())
                user_ids.erase(it);
        }
    }
    else
    {
        for (const auto & name : list.names)
        {
            auto id = find<User>(name);
            if (id)
                user_ids.push_back(*id);
            else
                role_ids.push_back(getID<Role>(name));
        }
        if (list.current_user && current_user)
            user_ids.push_back(*current_user);
    }
    return {std::move(user_ids), std::move(role_ids)};
}


std::vector<UUID> AccessControlManager::getGrantedRolesFromList(const UserPtr & user, const ASTRoleList & list) const
{
    std::vector<UUID> role_ids;
    if (list.all)
    {
        role_ids = user->granted_roles;
        for (const auto & except_name : list.except_names)
        {
            auto except_id = find<Role>(except_name);
            if (except_id)
            {
                auto it = boost::range::find(role_ids, except_id);
                if (it != role_ids.end())
                    role_ids.erase(it);
            }
        }
    }
    else
    {
        for (const auto & name : list.names)
        {
            auto id = getID<Role>(name);
            if (boost::range::find(user->granted_roles, id) == user->granted_roles.end())
                throw Exception(user->getName() + ": " + "Cannot set role " + backQuoteIfNeed(name) + " because it's not granted",
                                ErrorCodes::NOT_ENOUGH_PRIVILEGES);
            role_ids.push_back(id);
        }
    }
    return role_ids;
}


EnabledRolesPtr AccessControlManager::getEnabledRoles(const UserPtr & user, const std::vector<UUID> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (current_roles.empty())
        return nullptr;
    return getEnabledRoles(user, std::make_shared<std::vector<UUID>>(current_roles), on_change, subscription);
}


EnabledRolesPtr AccessControlManager::getEnabledRoles(const UserPtr & user, const std::shared_ptr<const std::vector<UUID>> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (!current_roles)
        return nullptr;
    auto watcher = std::make_shared<EnabledRolesWatcher>(*this, user, current_roles, on_change);
    if (on_change && subscription)
        *subscription = [watcher]{};
    return watcher->getEnabledRoles();
}


void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
}


std::shared_ptr<const AccessRightsContext> AccessControlManager::getAccessRightsContext(const UserPtr & user, const EnabledRolesPtr & enabled_roles, const ClientInfo & client_info, const Settings & settings, const String & current_database)
{
    return std::make_shared<AccessRightsContext>(user, enabled_roles, client_info, settings, current_database);
}


std::shared_ptr<QuotaContext> AccessControlManager::createQuotaContext(
    const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key)
{
    return quota_context_factory->createContext(user_name, address, custom_quota_key);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}


std::shared_ptr<RowPolicyContext> AccessControlManager::getRowPolicyContext(const String & user_name) const
{
    return row_policy_context_factory->createContext(user_name);
}

}
