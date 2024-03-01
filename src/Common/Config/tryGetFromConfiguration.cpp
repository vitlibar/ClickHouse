#include <Common/Config/tryGetFromConfiguration.h>
#include <Poco/NumberParser.h>
#include <Poco/String.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

bool tryGetBoolFromConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & key, bool & result)
{
    std::string str = config.getString(key, "");

    /// See AbstractConfiguration::parseBool().
	int n;
	if (Poco::NumberParser::tryParse(str, n))
    {
		result = (n != 0);
        return true;
    }
	else if ((Poco::icompare(str, "true") == 0) || (Poco::icompare(str, "yes") == 0) || (Poco::icompare(str, "on") == 0))
    {
        result = true;
        return true;
    }
	else if ((Poco::icompare(str, "false") == 0) || (Poco::icompare(str, "no") == 0) || (Poco::icompare(str, "off") == 0))
    {
        result = false;
        return true;
    }
    else
        return false;
}


bool canGetBoolFromConfiguration(const Poco::Util::AbstractConfiguration & config, const std::string & key)
{
    bool value;
    return tryGetBoolFromConfiguration(config, key, value);
}

}
