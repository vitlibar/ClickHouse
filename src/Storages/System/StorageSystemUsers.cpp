#include <Storages/System/StorageSystemUsers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/AccessFlags.h>


namespace DB
{
NamesAndTypesList StorageSystemUsers::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
        {"default_roles_all", std::make_shared<DataTypeUInt8>()},
        {"default_roles_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"default_roles_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    };
    return names_and_types;
}


void StorageSystemUsers::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<SettingsProfile>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUInt128 &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_default_roles_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_default_roles_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_default_roles_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_default_roles_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const ExtendedRoleSet & default_roles)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id);
        column_storage.insertData(storage_name.data(), storage_name.length());

        auto default_roles_ast = default_roles.toASTWithNames(access_control);
        column_default_roles_all.push_back(default_roles_ast->all);

        for (const auto & name : default_roles_ast->names)
            column_default_roles_list.insertData(name.data(), name.length());
        column_default_roles_list_offsets.push_back(column_default_roles_list.size());

        for (const auto & name : default_roles_ast->except_names)
            column_default_roles_except.insertData(name.data(), name.length());
        column_default_roles_except_offsets.push_back(column_default_roles_except.size());
    };

    for (const auto & id : ids)
    {
        auto user = access_control.tryRead<User>(id);
        if (!user)
            continue;

        const auto * storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(user->getName(), id, storage->getStorageName(), user->default_roles);
    }
}

}
