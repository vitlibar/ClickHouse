#include <Storages/System/StorageSystemGrantedRoles.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControlManager.h>
#include <Access/Role.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace
{
    enum class GranteeType
    {
        USER,
        ROLE,
    };

    DataTypeEnum8::Values getGranteeTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        enum_values.emplace_back("USER", static_cast<UInt8>(GranteeType::USER));
        enum_values.emplace_back("ROLE", static_cast<UInt8>(GranteeType::ROLE));
        return enum_values;
    }
}


NamesAndTypesList StorageSystemGrantedRoles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"grantee_name", std::make_shared<DataTypeString>()},
        {"grantee_type", std::make_shared<DataTypeEnum8>(getGranteeTypeEnumValues())},
        {"role_name", std::make_shared<DataTypeString>()},
        {"admin_option", std::make_shared<DataTypeUInt8>()},
        {"is_default", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemGrantedRoles::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<User>();
    boost::range::push_back(ids, access_control.findAll<Role>());

    size_t column_index = 0;
    auto & column_grantee_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_grantee_type = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_role_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_admin_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_default = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & grantee_name,
                       GranteeType grantee_type,
                       const String & role_name,
                       bool admin_option,
                       bool is_default)
    {
        column_grantee_name.insertData(grantee_name.data(), grantee_name.length());
        column_grantee_type.push_back(grantee_type);
        column_role_name.insertData(role_name.data(), role_name.length());
        column_admin_option.push_back(admin_option);
        column_is_default.push_back(is_default);
    };

    auto add_rows = [&](const String & grantee_name,
                        GranteeType grantee_type,
                        const GrantedRoles & granted_roles,
                        const ExtendedRoleSet * default_roles)
    {
        for (const auto & role_id : granted_roles.roles)
        {
            auto role_name = access_control.tryReadName(role_id);
            if (!role_name)
                continue;

            bool admin_option = granted_roles.roles_with_admin_option.contains(role_id);
            bool is_default = !default_roles || default_roles->match(role_id);
            add_row(grantee_name, grantee_type, *role_name, admin_option, is_default);
        }
    };

    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        const String & grantee_name = entity->getFullName();

        if (auto role = typeid_cast<RolePtr>(entity))
            add_rows(grantee_name, GranteeType::ROLE, role->granted_roles, nullptr);
        else if (auto user = typeid_cast<UserPtr>(entity))
            add_rows(grantee_name, GranteeType::USER, user->granted_roles, &user->default_roles);
        else
            continue;
    }
}

}
