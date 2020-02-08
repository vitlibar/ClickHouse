#pragma once

#include <Access/MultipleAccessStorage.h>
#include <Poco/AutoPtr.h>
#include <memory>


namespace Poco
{
    namespace Net
    {
        class IPAddress;
    }
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{
<<<<<<< HEAD
class AccessRightsContext;
using AccessRightsContextPtr = std::shared_ptr<const AccessRightsContext>;
class AccessRightsContextFactory;
class RowPolicyContext;
using RowPolicyContextPtr = std::shared_ptr<const RowPolicyContext>;
class RowPolicyContextFactory;
=======
struct User;
using UserPtr = std::shared_ptr<const User>;
struct Role;
using RolePtr = std::shared_ptr<const Role>;
using EnabledRolesPtr = std::shared_ptr<const std::vector<std::pair<UUID, RolePtr>>>;
>>>>>>> c55f214907... Introduce roles.
class QuotaContext;
using QuotaContextPtr = std::shared_ptr<const QuotaContext>;
class QuotaContextFactory;
struct QuotaUsageInfo;
class ClientInfo;
struct Settings;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);

<<<<<<< HEAD
    AccessRightsContextPtr getAccessRightsContext(const UUID & user_id, const Settings & settings, const String & current_database, const ClientInfo & client_info) const;
=======
    UserPtr getUser(const String & user_name, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr getUser(const UUID & user_id, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr authorizeAndGetUser(const String & user_name, const String & password, const Poco::Net::IPAddress & address, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    UserPtr authorizeAndGetUser(const UUID & user_id, const String & password, const Poco::Net::IPAddress & address, std::function<void(const UserPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;

    EnabledRolesPtr getEnabledRoles(const std::vector<UUID> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;
    EnabledRolesPtr getEnabledRoles(const std::shared_ptr<const std::vector<UUID>> & current_roles, std::function<void(const EnabledRolesPtr &)> on_change = {}, ext::scope_guard * subscription = nullptr) const;

    std::shared_ptr<const AccessRightsContext> getAccessRightsContext(const UserPtr & user, const EnabledRolesPtr & enabled_roles, const ClientInfo & client_info, const Settings & settings, const String & current_database);
>>>>>>> c55f214907... Introduce roles.

    RowPolicyContextPtr getRowPolicyContext(const UUID & user_id) const;

    QuotaContextPtr getQuotaContext(const UUID & user_id, const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const;
    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

private:
    std::unique_ptr<AccessRightsContextFactory> access_rights_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
};

}
