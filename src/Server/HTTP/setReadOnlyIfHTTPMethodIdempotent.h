#pragma once

#include <base/types.h>


namespace DB
{
class SettingsChanges;
struct Settings;

/// Sets readonly = 2 in `new_settings` if the current HTTP method is not HTTP POST and if readonly is not set already.
void setReadOnlyIfHTTPMethodIdempotent(SettingsChanges & new_settings, const Settings & current_settings, const String & http_method);

}
