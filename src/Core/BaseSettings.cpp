#include <Core/BaseSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

void BaseSettingsHelpers::writeString(const std::string_view & str, WriteBuffer & out)
{
    writeStringBinary(str, out);
}


String BaseSettingsHelpers::readString(ReadBuffer & in)
{
    String str;
    readStringBinary(str, in);
    return str;
}


void BaseSettingsHelpers::writeFlag(bool flag, WriteBuffer & out)
{
    out.write(flag);
}


bool BaseSettingsHelpers::readFlag(ReadBuffer & in)
{
    char c;
    in.readStrict(c);
    return c;
}


void BaseSettingsHelpers::throwSettingNotFound(const std::string_view & name)
{
    throw Exception("Unknown setting " + String{name}, ErrorCodes::UNKNOWN_SETTING);
}


void BaseSettingsHelpers::warningSettingNotFound(const std::string_view & name)
{
    static auto * log = &Poco::Logger::get("Settings");
    LOG_WARNING(log, "Unknown setting {}, skipping", name);
}

}
