#pragma once

#include <Access/SourceSyncAttributesStorage.h>
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
/// Implementation of IAttributesStorage which loads all from users.xml periodically.
class UsersConfigFileAttributesStorage : public SourceSyncAttributesStorage
{
public:
    UsersConfigFileAttributesStorage();
    ~UsersConfigFileAttributesStorage() override;

    void loadFromConfig(const Poco::AutoPtr<Poco::Util::AbstractConfiguration> & config_);

private:
    NamesAndTypes readNamesAndTypesFromSource() const override;
    Strings readNamesOfSettingsProfiles() const;
    Strings readNamesOfUsers() const;
    AttributesPtr readFromSource(const String & name, const Type & type) const override;
    AttributesPtr readSettingsProfile(const String & name) const;
    AttributesPtr readUser(const String & name) const;

    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
};
}
