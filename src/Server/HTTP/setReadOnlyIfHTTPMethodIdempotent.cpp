#include <Server/HTTP/setReadOnlyIfHTTPMethodIdempotent.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Core/Settings.h>


namespace DB
{

void setReadOnlyIfHTTPMethodIdempotent(SettingsChanges & new_settings, const Settings & current_settings, const String & http_method)
{
    /// Anything else beside HTTP POST should be readonly queries.
    if (http_method != HTTPServerRequest::HTTP_POST)
    {
        /// 'readonly' setting values mean:
        /// readonly = 0 - any query is allowed, client can change any setting.
        /// readonly = 1 - only readonly queries are allowed, client can't change settings.
        /// readonly = 2 - only readonly queries are allowed, client can change any setting except 'readonly'.
        if (current_settings.readonly == 0)
        {
            Field readonly_query_param;
            if (!new_settings.tryGet("readonly", readonly_query_param) || (SettingFieldUInt64(readonly_query_param).value == 0))
                new_settings.setSetting("readonly", static_cast<UInt64>(2));
        }
    }
}

}
