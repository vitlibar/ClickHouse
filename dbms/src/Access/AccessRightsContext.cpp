#include <Access/AccessRightsContext.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_ENOUGH_PRIVILEGES;
}


AccessRightsContext::AccessRightsContext()
{
    result_access.grant(AccessType::ALL);
}

AccessRightsContext::AccessRightsContext(const String & user_name_, const AccessRights & granted_to_user_, bool readonly_, bool allow_ddl_)
    : user_name(user_name_), granted_to_user(granted_to_user_), readonly(readonly_), allow_ddl(allow_ddl_)
{
    result_access = granted_to_user;
}


template <typename... Args>
void AccessRightsContext::checkImpl(const AccessType & access, const Args &... args) const
{
    if (!result_access.isGranted(access, args...))
        throw Exception(
            user_name + ": Not enough privileges. To perform this operation you should have "
                + AccessRights::grantToString(access, args...),
            ErrorCodes::NOT_ENOUGH_PRIVILEGES);
}

void AccessRightsContext::check(const AccessType & access) const { return checkImpl(access); }
void AccessRightsContext::check(const AccessType & access, const std::string_view & database) const { return checkImpl(access, database); }
void AccessRightsContext::check(const AccessType & access, const Strings & databases) const { return checkImpl(access, databases); }
void AccessRightsContext::check(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) const { return checkImpl(access, database, table_or_dictionary); }
void AccessRightsContext::check(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) const { return checkImpl(access, database, tables_or_dictionaries); }
void AccessRightsContext::check(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) const { return checkImpl(access, database, table_or_dictionary, column_or_attribute); }
void AccessRightsContext::check(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) const { return checkImpl(access, database, table_or_dictionary, columns_or_attributes); }

}
