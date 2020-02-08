#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
<<<<<<< HEAD
#include <Access/AccessRightsContextFactory.h>
=======
#include <Access/User.h>
#include <Access/EnabledRolesWatcher.h>
#include <Access/QuotaContextFactory.h>
>>>>>>> c55f214907... Introduce roles.
#include <Access/RowPolicyContextFactory.h>
#include <Access/QuotaContextFactory.h>


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
      access_rights_context_factory(std::make_unique<AccessRightsContextFactory>(*this)),
      row_policy_context_factory(std::make_unique<RowPolicyContextFactory>(*this)),
      quota_context_factory(std::make_unique<QuotaContextFactory>(*this))
{
}


AccessControlManager::~AccessControlManager()
{
}


<<<<<<< HEAD
=======
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


EnabledRolesPtr AccessControlManager::getEnabledRoles(const std::vector<UUID> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (current_roles.empty())
        return nullptr;
    return getEnabledRoles(std::make_shared<std::vector<UUID>>(current_roles), on_change, subscription);
}


EnabledRolesPtr AccessControlManager::getEnabledRoles(const std::shared_ptr<const std::vector<UUID>> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change, ext::scope_guard * subscription) const
{
    if (!current_roles)
        return nullptr;
    auto watcher = std::make_shared<EnabledRolesWatcher>(*this, current_roles, on_change);
    if (on_change && subscription)
        *subscription = [watcher]{};
    return watcher->getEnabledRoles();
}


>>>>>>> c55f214907... Introduce roles.
void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
}


<<<<<<< HEAD
AccessRightsContextPtr AccessControlManager::getAccessRightsContext(
    const UUID & user_id, const Settings & settings, const String & current_database, const ClientInfo & client_info) const
{
    return access_rights_context_factory->createContext(
        user_id, settings, current_database, client_info);
=======
std::shared_ptr<const AccessRightsContext> AccessControlManager::getAccessRightsContext(const UserPtr & user, const EnabledRolesPtr & enabled_roles, const ClientInfo & client_info, const Settings & settings, const String & current_database)
{
    return std::make_shared<AccessRightsContext>(user, enabled_roles, client_info, settings, current_database);
>>>>>>> c55f214907... Introduce roles.
}


RowPolicyContextPtr AccessControlManager::getRowPolicyContext(const UUID & user_id) const
{
    return row_policy_context_factory->createContext(user_id);
}


QuotaContextPtr AccessControlManager::getQuotaContext(
    const UUID & user_id, const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const
{
    return quota_context_factory->createContext(user_id, user_name, address, custom_quota_key);
}

std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}

}
