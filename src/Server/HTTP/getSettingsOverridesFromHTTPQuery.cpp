#include <Server/HTTP/getSettingsOverridesFromHTTPQuery.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Core/Settings.h>


namespace DB
{

SettingsChanges getSettingsOverridesFromHTTPQuery(const HTTPServerRequest & request, const HTMLForm & params, const Settings & current_settings,
                                                  const std::function<bool(const String & key)> & filter)
{
    SettingsChanges settings_changes;

    for (const auto & [key, value] : params)
    {
        /// Empty parameter appears when URL like ?&a=b or a=b&&c=d. Just skip them for user's convenience.
        if (key.empty())
            continue;

        if (filter && !filter(key))
            continue;

        settings_changes.push_back({key, value});
    }

    /// Anything else beside HTTP POST should be readonly queries.
    if (request.getMethod() != HTTPServerRequest::HTTP_POST)
    {
        /// In theory if initially readonly = 0, the client can change any setting and then set readonly
        /// to some other value.
        if (current_settings.readonly == 0)
        {
            std::optional<UInt64> readonly_value;
            for (const auto & setting_change : settings_changes)
            {
                UInt64 value;
                if ((setting_change.name == "readonly") && tryParse(value, setting_change.value.get<const String &>()))
                    readonly_value = value;
            }

            if (!readonly_value || (*readonly_value == 0))
                settings_changes.push_back({"readonly", Field{2}});
        }
    }

    return settings_changes;
}

}
