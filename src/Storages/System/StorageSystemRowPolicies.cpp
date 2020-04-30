#include <Storages/System/StorageSystemRowPolicies.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTExtendedRoleSet.h>
#include <Access/AccessControlManager.h>
#include <Access/AccessFlags.h>
#include <Access/RowPolicy.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{

NamesAndTypesList StorageSystemRowPolicies::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"full_name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
    };

    for (auto condition_type : ext::range_with_static_cast<RowPolicy::ConditionType>(RowPolicy::MAX_CONDITION_TYPE))
    {
        String column_name = RowPolicy::conditionTypeToString(condition_type);
        boost::to_lower(column_name);
        names_and_types.emplace_back(column_name, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()));
    }

    boost::range::push_back(names_and_types, NamesAndTypesList{
        {"select_filter", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"is_restrictive", std::make_shared<DataTypeUInt8>()},
        {"apply_to_all", std::make_shared<DataTypeUInt8>()},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
    });
    return names_and_types;
}


void StorageSystemRowPolicies::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    context.checkAccess(AccessType::SHOW_ROW_POLICIES);
    const auto & access_control = context.getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<RowPolicy>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_database = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_table = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_full_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUInt128 &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);

    ColumnString * column_condition[RowPolicy::MAX_CONDITION_TYPE];
    NullMap * column_condition_null_map[RowPolicy::MAX_CONDITION_TYPE];
    for (auto condition_type : ext::range_with_static_cast<RowPolicy::ConditionType>(RowPolicy::MAX_CONDITION_TYPE))
    {
        column_condition[condition_type] = &assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
        column_condition_null_map[condition_type] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    auto & column_is_restrictive = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const RowPolicy::FullNameParts & full_name_parts,
                       const UUID & id,
                       const String & storage_name,
                       const String * conditions,
                       bool is_restrictive,
                       const ExtendedRoleSet & apply_to)
    {
        column_name.insertData(full_name_parts.policy_name.data(), full_name_parts.policy_name.length());
        column_database.insertData(full_name_parts.database.data(), full_name_parts.database.length());
        column_table.insertData(full_name_parts.table_name.data(), full_name_parts.table_name.length());
        String full_name = full_name_parts.getFullName();
        column_full_name.insertData(full_name.data(), full_name.length());
        column_id.push_back(id);
        column_storage.insertData(storage_name.data(), storage_name.length());

        for (auto condition_type : ext::range_with_static_cast<RowPolicy::ConditionType>(RowPolicy::MAX_CONDITION_TYPE))
        {
            const String & condition = conditions[condition_type];
            if (condition.empty())
            {
                column_condition[condition_type]->insertDefault();
                column_condition_null_map[condition_type]->push_back(true);
            }
            else
            {
                column_condition[condition_type]->insertData(condition.data(), condition.length());
                column_condition_null_map[condition_type]->push_back(true);
            }
        }

        column_is_restrictive.push_back(is_restrictive);

        auto apply_to_ast = apply_to.toASTWithNames(access_control);
        column_apply_to_all.push_back(apply_to_ast->all);

        for (const auto & name : apply_to_ast->names)
            column_apply_to_list.insertData(name.data(), name.length());
        column_apply_to_list_offsets.push_back(column_apply_to_list.size());

        for (const auto & name : apply_to_ast->except_names)
            column_apply_to_except.insertData(name.data(), name.length());
        column_apply_to_except_offsets.push_back(column_apply_to_except.size());
    };

    for (const auto & id : ids)
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;

        const auto * storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(
            policy->getFullNameParts(),
            id,
            storage->getStorageName(),
            policy->conditions[RowPolicy::ConditionType::SELECT_FILTER],
            policy->isRestrictive(),
            policy->to_roles);
    }
}

}
