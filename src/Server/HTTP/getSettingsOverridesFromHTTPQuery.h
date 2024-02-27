#pragma once

#include <Common/SettingsChanges.h>
#include <functional>


namespace DB
{
class HTTPServerRequest;
class HTMLForm;
struct Settings;

/// Extracts overrides for settings values from a HTTP query.
/// The `filter` is used to check each setting's name before adding it to the result.
SettingsChanges getSettingsOverridesFromHTTPQuery(const HTTPServerRequest & request, const HTMLForm & params, const Settings & current_settings,
                                                  const std::function<bool(const String & key)> & filter = {});

}
