#include <Storages/TimeSeries/getTimeSeriesTableActualStructure.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCOMPATIBLE_COLUMNS;
}


ColumnsDescription getTimeSeriesTableActualStructure(
    const ContextPtr & local_context, const ASTCreateQuery & create_query, const ColumnsDescription & columns_from_create_query)
{
    ColumnsDescription res;

    /// `src` is the columns which are set in the create query explicitly.
    const ColumnsDescription & src = columns_from_create_query;

    /// Loads the TimeSeries storage settings from a create query.
    auto time_series_settings = std::make_shared<TimeSeriesSettings>();
    if (create_query.storage)
        time_series_settings->loadFromQuery(*create_query.storage);

    /// Column types can be extracted the target tables if they are not inner.
    auto data_table_id = create_query.getTargetTableId(TargetTableKind::kData);
    auto tags_table_id = create_query.getTargetTableId(TargetTableKind::kTags);
    auto metrics_table_id = create_query.getTargetTableId(TargetTableKind::kMetrics);

    StorageMetadataPtr data_table_metadata, tags_table_metadata, metrics_table_metadata; 

    const ColumnsDescription * data_table_columns = nullptr;
    const ColumnsDescription * tags_table_columns = nullptr;
    const ColumnsDescription * metrics_table_columns = nullptr;

    auto has_data_table_columns = [&]
    {
        if (!data_table_id)
            return false;
        if (!data_table_columns)
        {
            data_table_metadata = DatabaseCatalog::instance().getTable(data_table_id, local_context)->getInMemoryMetadataPtr();
            data_table_columns = &data_table_metadata->columns;
        }
        return true;
    };

    auto has_tags_table_columns = [&]
    {
        if (!tags_table_id)
            return false;
        if (!tags_table_columns)
        {
            tags_table_metadata = DatabaseCatalog::instance().getTable(tags_table_id, local_context)->getInMemoryMetadataPtr();
            tags_table_columns = &tags_table_metadata->columns;
        }
        return true;
    };

    auto has_metrics_table_columns = [&]
    {
        if (!metrics_table_id)
            return false;
        if (!metrics_table_columns)
        {
            metrics_table_metadata = DatabaseCatalog::instance().getTable(metrics_table_id, local_context)->getInMemoryMetadataPtr();
            metrics_table_columns = &metrics_table_metadata->columns;
        }
        return true;
    };

    /// Build the result list of columns.
    if (src.has(TimeSeriesColumnNames::kID))
        res.add(src.get(TimeSeriesColumnNames::kID));
    else if (has_data_table_columns() && data_table_columns->has(TimeSeriesColumnNames::kID))
        res.add(data_table_columns->get(TimeSeriesColumnNames::kID));
    else if (has_tags_table_columns() && tags_table_columns->has(TimeSeriesColumnNames::kID))
        res.add(tags_table_columns->get(TimeSeriesColumnNames::kID));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kID, std::make_shared<DataTypeUInt128>()});

    if (src.has(TimeSeriesColumnNames::kTimestamp))
        res.add(src.get(TimeSeriesColumnNames::kTimestamp));
    else if (has_data_table_columns() && data_table_columns->has(TimeSeriesColumnNames::kTimestamp))
        res.add(data_table_columns->get(TimeSeriesColumnNames::kTimestamp));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kTimestamp, std::make_shared<DataTypeDateTime64>(3)});

    if (src.has(TimeSeriesColumnNames::kValue))
        res.add(src.get(TimeSeriesColumnNames::kValue));
    else if (has_data_table_columns() && data_table_columns->has(TimeSeriesColumnNames::kValue))
        res.add(data_table_columns->get(TimeSeriesColumnNames::kValue));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kValue, std::make_shared<DataTypeFloat64>()});

    auto get_string_type = [] { return std::make_shared<DataTypeString>(); };
    auto get_low_cardinality_string_type = [] { return std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()); };

    if (src.has(TimeSeriesColumnNames::kMetricName))
        res.add(src.get(TimeSeriesColumnNames::kMetricName));
    else if (has_tags_table_columns() && tags_table_columns->has(TimeSeriesColumnNames::kMetricName))
        res.add(tags_table_columns->get(TimeSeriesColumnNames::kMetricName));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kMetricName, get_low_cardinality_string_type()});

    const Map & tags_to_columns = time_series_settings->tags_to_columns;
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        if (src.has(column_name))
            res.add(src.get(column_name));
        else if (has_tags_table_columns() && tags_table_columns->has(column_name))
            res.add(tags_table_columns->get(column_name));
        else
            res.add(ColumnDescription{column_name, get_string_type()});
    }

    if (src.has(TimeSeriesColumnNames::kTags))
        res.add(src.get(TimeSeriesColumnNames::kTags));
    else if (has_tags_table_columns() && tags_table_columns->has(TimeSeriesColumnNames::kTags))
        res.add(tags_table_columns->get(TimeSeriesColumnNames::kTags));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kTags, std::make_shared<DataTypeMap>(get_low_cardinality_string_type(), get_string_type())});

    if (src.has(TimeSeriesColumnNames::kMetricFamilyName))
        res.add(src.get(TimeSeriesColumnNames::kMetricFamilyName));
    else if (has_metrics_table_columns() && metrics_table_columns->has(TimeSeriesColumnNames::kMetricFamilyName))
        res.add(metrics_table_columns->get(TimeSeriesColumnNames::kMetricFamilyName));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kMetricFamilyName, get_string_type()});

    if (src.has(TimeSeriesColumnNames::kType))
        res.add(src.get(TimeSeriesColumnNames::kType));
    else if (has_metrics_table_columns() && metrics_table_columns->has(TimeSeriesColumnNames::kType))
        res.add(metrics_table_columns->get(TimeSeriesColumnNames::kType));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kType, get_low_cardinality_string_type()});

    if (src.has(TimeSeriesColumnNames::kUnit))
        res.add(src.get(TimeSeriesColumnNames::kUnit));
    else if (has_metrics_table_columns() && metrics_table_columns->has(TimeSeriesColumnNames::kUnit))
        res.add(metrics_table_columns->get(TimeSeriesColumnNames::kUnit));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kUnit, get_low_cardinality_string_type()});

    if (src.has(TimeSeriesColumnNames::kHelp))
        res.add(src.get(TimeSeriesColumnNames::kHelp));
    else if (has_metrics_table_columns() && metrics_table_columns->has(TimeSeriesColumnNames::kHelp))
        res.add(metrics_table_columns->get(TimeSeriesColumnNames::kHelp));
    else
        res.add(ColumnDescription{TimeSeriesColumnNames::kHelp, get_string_type()});

    const Map & extra_columns = time_series_settings->extra_columns;
    for (const auto & column_name_and_table : extra_columns)
    {
        const auto & tuple = column_name_and_table.safeGet<const Tuple &>();
        const auto & column_name = tuple.at(0).safeGet<String>();
        res.add(src.get(column_name));
    }

    /// All columns explicitly specified in the create query must be used.
    for (const auto & column : src)
    {
        if (!res.has(column.name))
            throw Exception(ErrorCodes::INCOMPATIBLE_COLUMNS, "Table {}.{} can't accept column {}",
                            create_query.getDatabase(), create_query.getTable(), column.name);
    }

    return res;
}

}
