#pragma once

#include <Access/MultipleAttributesStorage.h>
#include <Poco/AutoPtr.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{
/// Manages access control entities.
class AccessControlManager : public MultipleAttributesStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::AutoPtr<Poco::Util::AbstractConfiguration> & users_config);
};

}
