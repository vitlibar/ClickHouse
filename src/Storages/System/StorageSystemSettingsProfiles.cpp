#include <Storages/System/StorageSystemSettingsProfiles.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/SettingsProfile.h>
#include <Access/AccessFlags.h>


namespace DB
{
NamesAndTypesList StorageSystemSettingsProfiles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
        {"apply_to", std::make_shared<DataTypeString>()},
    };
    return names_and_types;
}


void StorageSystemSettingsProfiles::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_USERS | AccessType::SHOW_ROLES | AccessType::SHOW_SETTINGS_PROFILES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<SettingsProfile>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUInt128 &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_apply_to = assert_cast<ColumnString &>(*res_columns[column_index]);

    auto add_row = [&](const String & name,
                       const UUID & id,
                       const String & storage_name,
                       const ExtendedRoleSet & apply_to)
    {
        column_name.insertData(name.data(), name.length());
        column_id.push_back(id);
        column_storage.insertData(storage_name.data(), storage_name.length());

        String apply_to_str = apply_to.toStringWithNames(access_control);
        column_apply_to.insertData(apply_to_str.data(), apply_to_str.length());
    };

    for (const auto & id : ids)
    {
        auto profile = access_control.tryRead<SettingsProfile>(id);
        if (!profile)
            continue;

        const auto * storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(profile->getName(), id, storage->getStorageName(), profile->to_roles);
    }
}

}
