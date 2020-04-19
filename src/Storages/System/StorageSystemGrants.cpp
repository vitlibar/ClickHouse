#include <Storages/System/StorageSystemGrants.h>
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

    DataTypeEnum8::Values getAccessTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;

#define ADD_ACCESS_TYPE_ENUM_VALUE(name, aliases, node_type, parent_group_name) \
        enum_values.emplace_back(#name, static_cast<size_t>(AccessType::name));

        APPLY_FOR_ACCESS_TYPES(ADD_ACCESS_TYPE_ENUM_VALUE)

#undef ADD_ACCESS_TYPE_ENUM_VALUE

        return enum_values;
    }
}


NamesAndTypesList StorageSystemGrants::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"grantee_name", std::make_shared<DataTypeString>()},
        {"grantee_type", std::make_shared<DataTypeEnum8>(getGranteeTypeEnumValues())},
        {"access_type", std::make_shared<DataTypeEnum8>(getAccessTypeEnumValues())},
        {"database", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"table", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"column", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"is_partial_revoke", std::make_shared<DataTypeUInt8>()},
        {"grant_option", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemGrants::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<User>();
    boost::range::push_back(ids, access_control.findAll<Role>());

    size_t column_index = 0;
    auto & column_grantee_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_grantee_type = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_access_type = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_database = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_database_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_table = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_table_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_column = assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
    auto & column_column_null_map = assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    auto & column_is_partial_revoke = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_grant_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const String & grantee_name,
                       GranteeType grantee_type,
                       AccessType access_type,
                       const String * database,
                       const String * table,
                       const String * column,
                       bool is_partial_revoke,
                       bool grant_option)
    {
        column_grantee_name.insertData(grantee_name.data(), grantee_name.length());
        column_grantee_type.push_back(grantee_type);
        column_access_type.push_back(access_type);

        if (database)
        {
            column_database.insertData(database->data(), database->length());
            column_database_null_map.push_back(false);
        }
        else
        {
            column_database.insertDefault();
            column_database_null_map.push_back(true);
        }

        if (table)
        {
            column_table.insertData(table->data(), table->length());
            column_table_null_map.push_back(false);
        }
        else
        {
            column_table.insertDefault();
            column_table_null_map.push_back(true);
        }

        if (column)
        {
            column_column.insertData(column->data(), column->length());
            column_column_null_map.push_back(false);
        }
        else
        {
            column_column.insertDefault();
            column_column_null_map.push_back(true);
        }

        column_is_partial_revoke.push_back(is_partial_revoke);
        column_grant_option.push_back(grant_option);
    };

    auto add_rows = [&](const String & grantee_name,
                        GranteeType grantee_type,
                        const AccessRightsElements & elements,
                        bool is_partial_revoke,
                        bool grant_option)
    {
        for (const auto & element : elements)
        {
            auto access_types = element.access_flags.toAccessTypes();
            if (access_types.empty() || (!element.any_column && element.columns.empty()))
                continue;

            const auto * database = element.any_database ? nullptr : &element.database;
            const auto * table = element.any_table ? nullptr : &element.table;

            if (element.any_column)
            {
                for (const auto & access_type : access_types)
                    add_row(grantee_name, grantee_type, access_type, database, table, nullptr, is_partial_revoke, grant_option);
            }
            else
            {
                for (const auto & access_type : access_types)
                    for (const auto & column : element.columns)
                        add_row(grantee_name, grantee_type, access_type, database, table, &column, is_partial_revoke, grant_option);
            }
        }
    };

    for (const auto & id : ids)
    {
        auto entity = access_control.tryRead(id);
        if (!entity)
            continue;

        const String & grantee_name = entity->getFullName();
        const GrantedAccess * access = nullptr;
        GranteeType grantee_type;
        if (auto role = typeid_cast<RolePtr>(entity))
        {
            access = &role->access;
            grantee_type = GranteeType::ROLE;
        }
        else if (auto user = typeid_cast<UserPtr>(entity))
        {
            access = &user->access;
            grantee_type = GranteeType::USER;
        }
        else
            continue;

        auto grants = access->getGrantsAndPartialRevokes();
        add_rows(grantee_name, grantee_type, grants.grants, false, false);
        add_rows(grantee_name, grantee_type, grants.revokes, true, false);
        add_rows(grantee_name, grantee_type, grants.grants_with_grant_option, false, true);
        add_rows(grantee_name, grantee_type, grants.revokes_grant_option, true, true);
    }
}

}
