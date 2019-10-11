#pragma once

#include <Access/IAttributes.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <unordered_map>
#include <unordered_set>


namespace DB
{
struct User : public IAttributes
{
    /// Required password.
    Authentication authentication;

    String profile;
    String quota;

    AllowedClientHosts allowed_client_hosts;

    /// List of allowed databases.
    using DatabaseSet = std::unordered_set<std::string>;
    DatabaseSet databases;

    /// List of allowed dictionaries.
    using DictionarySet = std::unordered_set<std::string>;
    DictionarySet dictionaries;

    /// Table properties.
    using PropertyMap = std::unordered_map<std::string /* name */, std::string /* value */>;
    using TableMap = std::unordered_map<std::string /* table */, PropertyMap /* properties */>;
    using DatabaseMap = std::unordered_map<std::string /* database */, TableMap /* tables */>;
    DatabaseMap table_props;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<User>(); }
    bool equal(const IAttributes & other) const override;

    bool hasAccessToDatabase(const String & database_name) const;
    bool hasAccessToDictionary(const String & dictionary_name) const;
};
}
