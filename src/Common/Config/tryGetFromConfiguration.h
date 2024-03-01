#pragma once

#include <string>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{

/// Tries to extract a boolean value of the property with the given name.
/// Returns false if the key does not exists or its value is not a boolean.
bool tryGetBoolFromConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool & result);

/// Checks whether the configuration has a property with the given name and its value can be parsed as a boolean.
/// Returns false if the key does not exists or its value is not a boolean.
bool canGetBoolFromConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & key);

}
