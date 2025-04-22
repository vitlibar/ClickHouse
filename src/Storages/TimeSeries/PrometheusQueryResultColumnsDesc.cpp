#include <Storages/TimeSeries/getPrometheusQueryResultColumnsDesc.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/ParsedPrometheusQuery.h>


namespace DB
{
using ResultType = ParsedPrometheusQuery::ResultType;

ColumnsDescription getPrometheusQueryOutputColumnsDesc(
    const ParsedPrometheusQuery & parsed_promql_query, const StorageID & time_series_storage_id, const ContextPtr & context)
{
    StorageMetadataPtr data_table_metadata;
    DataTypePtr time_type;
    DataTypePtr scalar_type;

    auto result_type = parsed_promql_query.getResultType();

    if ((result_type == ResultType::INSTANT_VECTOR) || (result_type == ResultType::RANGE_VECTOR) || (result_type == ResultType::SCALAR))
    {
        auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
        data_table_metadata = time_series_storage->getTargetTable(ViewTarget::Data, context)->getInMemoryMetadataPtr();
        time_type = data_table_metadata->columns.getPhysical("timestamp").type;
        scalar_type = data_table_metadata->columns.getPhysical("value").type;
    }

    switch (result_type)
    {
        case ResultType::INSTANT_VECTOR:
        {
            return ColumnsDescription{NamesAndTypesList{
                {"metric_name", std::make_shared<DataTypeString>()},
                {"tags", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
                {"timestamp", time_type},
                {"value", scalar_type},
            }};
        }
        case ResultType::RANGE_VECTOR:
        {
            return ColumnsDescription{NamesAndTypesList{
                {"metric_name", std::make_shared<DataTypeString>()},
                {"tags", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>())},
                {"time_series", std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                    DataTypes{time_type, scalar_type},
                    Strings{"timestamp", "value"}))},
            }};
        }
        case ResultType::SCALAR:
        {
            return ColumnsDescription{NamesAndTypesList{
                {"scalar", scalar_type},
            }};
        }
        case ResultType::STRING:
        {
            return ColumnsDescription{NamesAndTypesList{
                {"string", std::make_shared<DataTypeString>()},
            }};
        }
    }

    UNREACHABLE();
}
}
