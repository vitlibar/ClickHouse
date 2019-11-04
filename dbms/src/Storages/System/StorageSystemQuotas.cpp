#include <Storages/System/StorageSystemQuotas.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <Access/AccessControlManager.h>
#include <Access/QuotaUsageContext.h>
#include <ext/range.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
namespace
{
    DataTypeEnum8::Values getKeyTypeEnumValues()
    {
        DataTypeEnum8::Values enum_values;
        for (auto key_type : ext::range_with_static_cast<Quota::KeyType>(Quota::MAX_KEY_TYPE))
            enum_values.push_back({Quota::getNameOfKeyType(key_type), static_cast<UInt8>(key_type)});
        return enum_values;
    }
}


NamesAndTypesList StorageSystemQuotas::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"name", std::make_shared<DataTypeString>()},
        {"id", std::make_shared<DataTypeUUID>()},
        {"source", std::make_shared<DataTypeString>()},
        {"key_type", std::make_shared<DataTypeEnum8>(getKeyTypeEnumValues())},
        {"key", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"duration", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
        {"randomize_interval", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>())},
        {"end_of_interval", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime>())}};

    for (auto resource_type : ext::range_with_static_cast<Quota::ResourceType>(Quota::MAX_RESOURCE_TYPE))
    {
        DataTypePtr data_type;
        if (resource_type == Quota::EXECUTION_TIME)
            data_type = std::make_shared<DataTypeFloat64>();
        else
            data_type = std::make_shared<DataTypeUInt64>();

        String column_name = Quota::resourceTypeToColumnName(resource_type);
        names_and_types.push_back({column_name, std::make_shared<DataTypeNullable>(data_type)});
        names_and_types.push_back({String("max_") + column_name, std::make_shared<DataTypeNullable>(data_type)});
    }
    return names_and_types;
}


void StorageSystemQuotas::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
    const auto & access_control = context.getAccessControlManager();

    std::vector<QuotaUsageInfo> infos = access_control.getQuotasUsageInfo();
    std::unordered_set<UUID> ids;
    boost::range::copy(access_control.findAll<Quota>(), std::inserter(ids, ids.begin()));

    /// Insert rows for quotas in use.
    for (const auto & info : infos)
    {
        ids.erase(info.quota_id);
        const auto * storage = access_control.findStorage(info.quota_id);
        String storage_name = storage ? storage->getStorageName() : "";

        auto insert_row = [&](const QuotaUsageInfo::Interval * interval)
        {
            size_t i = 0;
            res_columns[i++]->insert(info.quota_name);
            res_columns[i++]->insert(info.quota_id);
            res_columns[i++]->insert(storage_name);
            res_columns[i++]->insert(static_cast<UInt8>(info.quota_key_type));
            res_columns[i++]->insert(info.quota_key);
            if (interval)
            {
                res_columns[i++]->insert(std::chrono::seconds{interval->duration}.count());
                res_columns[i++]->insert(static_cast<UInt8>(interval->randomize_interval));
                res_columns[i++]->insert(std::chrono::system_clock::to_time_t(interval->end_of_interval));
                for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
                {
                    if (resource_type == Quota::EXECUTION_TIME)
                    {
                        res_columns[i++]->insert(Quota::executionTimeToSeconds(interval->used[resource_type]));
                        res_columns[i++]->insert(Quota::executionTimeToSeconds(interval->max[resource_type]));
                    }
                    else
                    {
                        res_columns[i++]->insert(interval->used[resource_type]);
                        res_columns[i++]->insert(interval->max[resource_type]);
                    }
                }
            }
            else
            {
                for (size_t j = 0; j != Quota::MAX_RESOURCE_TYPE * 2 + 3; ++j)
                    res_columns[i++]->insertDefault();
            }
        };

        if (info.intervals.empty())
            insert_row(nullptr);
        else
            for (const auto & interval : info.intervals)
                insert_row(&interval);
    }

    /// Insert rows for other quotas.
    for (const auto & id : ids)
    {
        auto quota = access_control.tryRead<Quota>(id);
        if (!quota)
            continue;
        const auto * storage = access_control.findStorage(id);
        String storage_name = storage ? storage->getStorageName() : "";

        auto insert_row = [&](const Quota::Limits * limits)
        {
            size_t i = 0;
            res_columns[i++]->insert(quota->getName());
            res_columns[i++]->insert(id);
            res_columns[i++]->insert(storage_name);
            res_columns[i++]->insert(static_cast<UInt8>(quota->key_type));
            res_columns[i++]->insertDefault();
            if (limits)
            {
                res_columns[i++]->insert(std::chrono::seconds{limits->duration}.count());
                res_columns[i++]->insert(static_cast<UInt8>(limits->randomize_interval));
                res_columns[i++]->insertDefault();
                for (auto resource_type : ext::range(Quota::MAX_RESOURCE_TYPE))
                {
                    res_columns[i++]->insertDefault();
                    if (resource_type == Quota::EXECUTION_TIME)
                        res_columns[i++]->insert(Quota::executionTimeToSeconds(limits->max[resource_type]));
                    else
                        res_columns[i++]->insert(limits->max[resource_type]);
                }
            }
            else
            {
                for (size_t j = 0; j != Quota::MAX_RESOURCE_TYPE * 2 + 3; ++j)
                    res_columns[i++]->insertDefault();
            }
        };

        if (quota->all_limits.empty())
            insert_row(nullptr);
        else
            for (const auto & limits : quota->all_limits)
                insert_row(&limits);
    }
}
}
