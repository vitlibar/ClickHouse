#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/AccessRightsContextFactory.h>
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


void AccessControlManager::loadFromConfig(const Poco::Util::AbstractConfiguration & users_config)
{
    auto & users_config_access_storage = dynamic_cast<UsersConfigAccessStorage &>(getStorageByIndex(1));
    users_config_access_storage.loadFromConfig(users_config);
}


std::shared_ptr<const AccessRightsContext> AccessControlManager::getAccessRightsContext(
    const UUID & user_id,
    const Settings & settings,
    const String & current_database,
    const ClientInfo & client_info,
    bool use_access_rights_for_initial_user)
{
    return access_rights_context_factory->createContext(
        user_id, settings, current_database, client_info, use_access_rights_for_initial_user);
}


std::shared_ptr<const RowPolicyContext> AccessControlManager::getRowPolicyContext(const UUID & user_id) const
{
    return row_policy_context_factory->createContext(user_id);
}


std::shared_ptr<const QuotaContext> AccessControlManager::getQuotaContext(
    const UUID & user_id, const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key) const
{
    return quota_context_factory->createContext(user_id, user_name, address, custom_quota_key);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotaUsageInfo() const
{
    return quota_context_factory->getUsageInfo();
}

}
