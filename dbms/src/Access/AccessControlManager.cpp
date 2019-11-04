#include <Access/AccessControlManager.h>
#include <Access/MultipleAccessStorage.h>
#include <Access/MemoryAccessStorage.h>
#include <Access/UsersConfigAccessStorage.h>
#include <Access/QuotaUsageManager.h>


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
      quota_usage_manager(std::make_unique<QuotaUsageManager>(*this))
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


std::shared_ptr<QuotaUsageContext> AccessControlManager::getQuotaUsageContext(
    const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key, const std::vector<UUID> & quota_ids)
{
    return quota_usage_manager->getContext(user_name, address, custom_quota_key, quota_ids);
}


std::vector<QuotaUsageInfo> AccessControlManager::getQuotasUsageInfo() const
{
    return quota_usage_manager->getInfo();
}
}
