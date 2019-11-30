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
class QuotaUsageContext;
class QuotaUsageManager;
struct QuotaUsageInfo;


/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);

    std::shared_ptr<QuotaUsageContext>
    getQuotaUsageContext(const String & user_name, const Poco::Net::IPAddress & address, const String & custom_quota_key);

    std::vector<QuotaUsageInfo> getQuotasUsageInfo() const;

private:
    std::unique_ptr<QuotaUsageManager> quota_usage_manager;
};

}
