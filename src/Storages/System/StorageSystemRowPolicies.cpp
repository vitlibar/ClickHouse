#include <Storages/System/StorageSystemRowPolicies.h>
#include <Access/AccessControlManager.h>
#include <Access/Common/AccessFlags.h>
#include <Access/RowPolicy.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Context.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <base/range.h>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
using FilterType = RowPolicy::FilterType;
using FilterTypeInfo = RowPolicy::FilterTypeInfo;


NamesAndTypesList StorageSystemRowPolicies::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"short_name", std::make_shared<DataTypeString>()},
        {"database", std::make_shared<DataTypeString>()},
        {"table", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"storage", std::make_shared<DataTypeString>()},
    };

    for (auto type : collections::range(FilterType::MAX))
    {
        const String & column_name = FilterTypeInfo::get(type).name;
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())});
    }

    NamesAndTypesList extra_names_and_types{
        {"is_restrictive", std::make_shared<DataTypeUInt8>()},
        {"apply_to_all", std::make_shared<DataTypeUInt8>()},
        {"apply_to_list", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
        {"apply_to_except", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())}
    };

    boost::range::push_back(names_and_types, std::move(extra_names_and_types));
    return names_and_types;
}


void StorageSystemRowPolicies::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    context->checkAccess(AccessType::SHOW_ROW_POLICIES);
    const auto & access_control = context->getAccessControlManager();
    std::vector<UUID> ids = access_control.findAll<RowPolicy>();

    size_t column_index = 0;
    auto & column_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_short_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_database = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_table = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_id = assert_cast<ColumnUUID &>(*res_columns[column_index++]).getData();
    auto & column_storage = assert_cast<ColumnString &>(*res_columns[column_index++]);

    constexpr auto num_filters = static_cast<size_t>(FilterType::MAX);
    ColumnString * column_filter[num_filters];
    NullMap * column_filter_null_map[num_filters];
    for (size_t type = 0; type != num_filters; ++type)
    {
        column_filter[type] = &assert_cast<ColumnString &>(assert_cast<ColumnNullable &>(*res_columns[column_index]).getNestedColumn());
        column_filter_null_map[type] = &assert_cast<ColumnNullable &>(*res_columns[column_index++]).getNullMapData();
    }

    auto & column_is_restrictive = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_all = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_apply_to_list = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_list_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();
    auto & column_apply_to_except = assert_cast<ColumnString &>(assert_cast<ColumnArray &>(*res_columns[column_index]).getData());
    auto & column_apply_to_except_offsets = assert_cast<ColumnArray &>(*res_columns[column_index++]).getOffsets();

    auto add_row = [&](const RowPolicyName & name,
                       const UUID & id,
                       const String & storage_name,
                       const RowPolicy::Filters & filters,
                       bool is_restrictive,
                       const RolesOrUsersSet & apply_to)
    {
        String full_name = name.toString();
        column_name.insertData(full_name.data(), full_name.length());
        column_short_name.insertData(name.short_name.data(), name.short_name.length());
        column_database.insertData(name.database.data(), name.database.length());
        column_table.insertData(name.table_name.data(), name.table_name.length());
        column_id.push_back(id.toUnderType());
        column_storage.insertData(storage_name.data(), storage_name.length());

        for (size_t type = 0; type != num_filters; ++type)
        {
            const String & filter = filters[type];
            if (filter.empty())
            {
                column_filter[type]->insertDefault();
                column_filter_null_map[type]->push_back(true);
            }
            else
            {
                column_filter[type]->insertData(filter.data(), filter.length());
                column_filter_null_map[type]->push_back(false);
            }
        }

        column_is_restrictive.push_back(is_restrictive);

        auto apply_to_ast = apply_to.toASTWithNames(access_control);
        column_apply_to_all.push_back(apply_to_ast->all);

        for (const auto & role_name : apply_to_ast->names)
            column_apply_to_list.insertData(role_name.data(), role_name.length());
        column_apply_to_list_offsets.push_back(column_apply_to_list.size());

        for (const auto & role_name : apply_to_ast->except_names)
            column_apply_to_except.insertData(role_name.data(), role_name.length());
        column_apply_to_except_offsets.push_back(column_apply_to_except.size());
    };

    for (const auto & id : ids)
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy)
            continue;
        auto storage = access_control.findStorage(id);
        if (!storage)
            continue;

        add_row(policy->getName(), id, storage->getStorageName(), policy->getFilters(), policy->isRestrictive(), policy->to_roles);
    }
}
}
