#include <Access/AccessControlManager.h>
#include <Access/MultipleAttributesStorage.h>
#include <Access/MemoryAttributesStorage.h>
#include <Access/UsersConfigFileAttributesStorage.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t SETTINGS_PROFILE_NAMESPACE_IDX = 0;
}


namespace
{
    std::vector<std::unique_ptr<IAttributesStorage>> createStorages()
    {
        std::vector<std::unique_ptr<IAttributesStorage>> list;
        list.emplace_back(std::make_unique<MemoryAttributesStorage>());
        list.emplace_back(std::make_unique<UsersConfigFileAttributesStorage>());
        return list;
    }
}


AccessControlManager::AccessControlManager()
    : MultipleAttributesStorage("AccessControl", createStorages())
{
}


AccessControlManager::~AccessControlManager()
{
}


void AccessControlManager::loadFromConfig(const Poco::AutoPtr<Poco::Util::AbstractConfiguration> & users_config)
{
    auto & users_config_file_attributes_storage = dynamic_cast<UsersConfigFileAttributesStorage &>(getNestedStorages(1));
    users_config_file_attributes_storage.loadFromConfig(users_config);
}

}
