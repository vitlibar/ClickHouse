#include <Access/ContextAccessParams.h>
#include <Core/Settings.h>


namespace DB
{

ContextAccessParams::ContextAccessParams(
    const std::optional<UUID> user_id_,
    bool full_access_,
    bool use_default_roles_,
    const std::shared_ptr<const std::vector<UUID>> & current_roles_,
    const Settings & settings_,
    const String & current_database_,
    const ClientInfo & client_info_)
    : user_id(user_id_)
    , full_access(full_access_)
    , use_default_roles(use_default_roles_)
    , current_roles(current_roles_.get())
    , readonly(settings_.readonly)
    , allow_ddl(settings_.allow_ddl)
    , allow_introspection(settings_.allow_introspection_functions)
    , current_database(current_database_)
    , interface(client_info_.interface)
    , http_method(client_info_.http_method)
    , address(client_info_.current_address.host())
    , forwarded_address(client_info_.getLastForwardedFor())
    , quota_key(client_info_.quota_key)
    , initial_user((client_info_.initial_user != client_info_.current_user) ? client_info_.initial_user : "")
{
}

bool operator <(const ContextAccessParams & left, const ContextAccessParams & right)
{
    #define CONTEXT_ACCESS_PARAMS_LESS(name) \
        if (left.name != right.name) \
            return left.name < right.name;

    CONTEXT_ACCESS_PARAMS_LESS(user_id)
    CONTEXT_ACCESS_PARAMS_LESS(full_access)
    CONTEXT_ACCESS_PARAMS_LESS(use_default_roles)
    CONTEXT_ACCESS_PARAMS_LESS(current_roles)
    CONTEXT_ACCESS_PARAMS_LESS(readonly)
    CONTEXT_ACCESS_PARAMS_LESS(allow_ddl)
    CONTEXT_ACCESS_PARAMS_LESS(allow_introspection)
    CONTEXT_ACCESS_PARAMS_LESS(current_database)
    CONTEXT_ACCESS_PARAMS_LESS(interface)
    CONTEXT_ACCESS_PARAMS_LESS(http_method)
    CONTEXT_ACCESS_PARAMS_LESS(address)
    CONTEXT_ACCESS_PARAMS_LESS(forwarded_address)
    CONTEXT_ACCESS_PARAMS_LESS(quota_key)
    CONTEXT_ACCESS_PARAMS_LESS(initial_user)

    #undef CONTEXT_ACCESS_PARAMS_LESS

    return true;
}

bool operator ==(const ContextAccessParams & left, const ContextAccessParams & right)
{
    #define CONTEXT_ACCESS_PARAMS_EQUAL(name) \
        if (left.name != right.name) \
            return false;

    CONTEXT_ACCESS_PARAMS_EQUAL(user_id)
    CONTEXT_ACCESS_PARAMS_EQUAL(full_access)
    CONTEXT_ACCESS_PARAMS_EQUAL(use_default_roles)
    CONTEXT_ACCESS_PARAMS_EQUAL(current_roles)
    CONTEXT_ACCESS_PARAMS_EQUAL(readonly)
    CONTEXT_ACCESS_PARAMS_EQUAL(allow_ddl)
    CONTEXT_ACCESS_PARAMS_EQUAL(allow_introspection)
    CONTEXT_ACCESS_PARAMS_EQUAL(current_database)
    CONTEXT_ACCESS_PARAMS_EQUAL(interface)
    CONTEXT_ACCESS_PARAMS_EQUAL(http_method)
    CONTEXT_ACCESS_PARAMS_EQUAL(address)
    CONTEXT_ACCESS_PARAMS_EQUAL(forwarded_address)
    CONTEXT_ACCESS_PARAMS_EQUAL(quota_key)
    CONTEXT_ACCESS_PARAMS_EQUAL(initial_user)

    #undef CONTEXT_ACCESS_PARAMS_EQUAL

    return true;
}

bool ContextAccessParams::dependsOnSettingName(std::string_view setting_name)
{
    return (setting_name == "readonly") || (setting_name == "allow_ddl") || (setting_name == "allow_introspection_functions");
}

}
