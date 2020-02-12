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
class AccessRightsContext;
class AccessRightsContextFactory;
class RowPolicyContext;
class RowPolicyContextFactory;
class QuotaContext;
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

    std::shared_ptr<const AccessRightsContext> getAccessRightsContext(const UUID & user_id, const Settings & settings, const String & current_database, const ClientInfo & client_info, bool use_access_rights_for_initial_user);

    std::shared_ptr<const RowPolicyContext> getRowPolicyContext(const UUID & user_id) const;

    std::shared_ptr<const QuotaContext> getQuotaContext(
        const UUID & user_id, const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const;

    std::vector<QuotaUsageInfo> getQuotaUsageInfo() const;

private:
    std::unique_ptr<AccessRightsContextFactory> access_rights_context_factory;
    std::unique_ptr<RowPolicyContextFactory> row_policy_context_factory;
    std::unique_ptr<QuotaContextFactory> quota_context_factory;
};

}
