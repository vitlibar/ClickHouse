#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsBool aggregate_min_time_and_max_time;
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsDataType scalar_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsDataType timestamp_type;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}


namespace
{
    /// Returns a column description of an inner table.
    ColumnsDescription getInnerTableColumnsDescription(
        ViewTarget::Kind inner_table_kind,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings)
    {
        ColumnsDescription columns;

        /// Lambda to copy a column from time_series_columns to the destination list if it's available.
        auto copy_column = [&](const String & name) -> bool
        {
            if (!time_series_columns.has(name))
                return false;
            columns.add(time_series_columns.get(name));
            return true;
        };

        switch (inner_table_kind)
        {
            case ViewTarget::Samples:
            {
                /// Column "id".
                if (!copy_column(TimeSeriesColumnNames::ID))
                    columns.add({TimeSeriesColumnNames::ID, time_series_settings[TimeSeriesSetting::id_type]});

                columns.modify(TimeSeriesColumnNames::ID, [&](ColumnDescription & id_column)
                {
                    /// Reset the default expression for the column "id".
                    /// This is needed in case we copied it from the main TimeSeries table.
                    /// The expression for calculating the identifier of a time series can be transferred only to the "tags" inner table
                    /// (because it usually depends on columns like "metric_name" or "all_tags").
                    id_column.default_desc = {};
                });

                /// Column "timestamp".
                if (!copy_column(TimeSeriesColumnNames::Timestamp))
                    columns.add({TimeSeriesColumnNames::Timestamp, time_series_settings[TimeSeriesSetting::timestamp_type]});

                /// Column "value".
                if (!copy_column(TimeSeriesColumnNames::Value))
                    columns.add({TimeSeriesColumnNames::Value, time_series_settings[TimeSeriesSetting::scalar_type]});

                break;
            }

            case ViewTarget::Tags:
            {
                /// Column "id".
                if (!copy_column(TimeSeriesColumnNames::ID))
                    columns.add({TimeSeriesColumnNames::ID, time_series_settings[TimeSeriesSetting::id_type]});

                columns.modify(TimeSeriesColumnNames::ID, [&](ColumnDescription & id_column)
                {
                    /// Assign to the expression for calculating the identifier of a time series from "metric_names" and "tags".
                    id_column.default_desc.expression = time_series_settings[TimeSeriesSetting::id_generator].value;
                    id_column.default_desc.kind = ColumnDefaultKind::Default;
                });

                /// Column "metric_name".
                if (!copy_column(TimeSeriesColumnNames::MetricName))
                    columns.add({TimeSeriesColumnNames::MetricName,
                        std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    if (!copy_column(column_name))
                        columns.add({column_name, std::make_shared<DataTypeString>()});
                }

                /// Column "tags".
                if (!copy_column(TimeSeriesColumnNames::Tags))
                    columns.add({TimeSeriesColumnNames::Tags,
                        std::make_shared<DataTypeMap>(
                            std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                            std::make_shared<DataTypeString>())});

                /// Column "all_tags".
                if (time_series_settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
                {
                    if (!copy_column(TimeSeriesColumnNames::AllTags))
                        columns.add({TimeSeriesColumnNames::AllTags,
                            std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())});
                    /// Column "all_tags" is here only to calculate the identifier of a time series for the "id" column, so it can be ephemeral.
                    columns.modify(TimeSeriesColumnNames::AllTags, [](ColumnDescription & col)
                    {
                        col.default_desc.kind = ColumnDefaultKind::Ephemeral;
                        col.default_desc.ephemeral_default = true;
                    });
                }

                /// Columns "min_time" and "max_time".
                if (time_series_settings[TimeSeriesSetting::store_min_time_and_max_time])
                {
                    if (time_series_settings[TimeSeriesSetting::aggregate_min_time_and_max_time])
                    {
                        /// When aggregation is enabled the columns need a custom SimpleAggregateFunction type,
                        /// which is not present in time_series_columns, so they must always be built from settings.
                        DataTypePtr min_time_type = std::make_shared<DataTypeNullable>(time_series_settings[TimeSeriesSetting::timestamp_type]);
                        DataTypePtr max_time_type = min_time_type;
                        AggregateFunctionProperties properties;
                        auto min_function = AggregateFunctionFactory::instance().get("min", NullsAction::EMPTY, {min_time_type}, {}, properties);
                        auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(min_function, DataTypes{min_time_type}, Array{});
                        min_time_type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));

                        auto max_function = AggregateFunctionFactory::instance().get("max", NullsAction::EMPTY, {max_time_type}, {}, properties);
                        custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(max_function, DataTypes{max_time_type}, Array{});
                        max_time_type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));

                        columns.add({TimeSeriesColumnNames::MinTime, std::move(min_time_type)});
                        columns.add({TimeSeriesColumnNames::MaxTime, std::move(max_time_type)});
                    }
                    else
                    {
                        DataTypePtr nullable_timestamp_type = std::make_shared<DataTypeNullable>(time_series_settings[TimeSeriesSetting::timestamp_type]);
                        if (!copy_column(TimeSeriesColumnNames::MinTime))
                            columns.add({TimeSeriesColumnNames::MinTime, nullable_timestamp_type});
                        if (!copy_column(TimeSeriesColumnNames::MaxTime))
                            columns.add({TimeSeriesColumnNames::MaxTime, nullable_timestamp_type});
                    }
                }

                break;
            }

            case ViewTarget::Metrics:
            {
                if (!copy_column(TimeSeriesColumnNames::MetricFamilyName))
                    columns.add({TimeSeriesColumnNames::MetricFamilyName, std::make_shared<DataTypeString>()});
                if (!copy_column(TimeSeriesColumnNames::Type))
                    columns.add({TimeSeriesColumnNames::Type, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
                if (!copy_column(TimeSeriesColumnNames::Unit))
                    columns.add({TimeSeriesColumnNames::Unit, std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>())});
                if (!copy_column(TimeSeriesColumnNames::Help))
                    columns.add({TimeSeriesColumnNames::Help, std::make_shared<DataTypeString>()});
                break;
            }

            default:
                UNREACHABLE();
        }

        return columns;
    }

    /// Generates a CREATE TABLE query for an inner table.
    boost::intrusive_ptr<ASTCreateQuery> getInnerTableCreateQuery(
        ViewTarget::Kind inner_table_kind,
        const UUID & inner_table_uuid,
        boost::intrusive_ptr<ASTStorage> inner_storage_def,
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings)
    {
        auto manual_create_query = make_intrusive<ASTCreateQuery>();

        manual_create_query->setDatabase(time_series_storage_id.getDatabaseName());
        manual_create_query->setTable(getTimeSeriesInnerTableName(inner_table_kind, time_series_storage_id));
        manual_create_query->uuid = inner_table_uuid;
        manual_create_query->has_uuid = inner_table_uuid != UUIDHelpers::Nil;

        auto new_columns_list = make_intrusive<ASTColumns>();
        new_columns_list->set(
            new_columns_list->columns,
            InterpreterCreateQuery::formatColumns(getInnerTableColumnsDescription(inner_table_kind, time_series_columns, time_series_settings)));
        manual_create_query->set(manual_create_query->columns_list, new_columns_list);

        if (inner_storage_def)
            manual_create_query->set(manual_create_query->storage, inner_storage_def->clone());

        return manual_create_query;
    }
}


void createTimeSeriesInnerTable(
    ViewTarget::Kind inner_table_kind,
    const UUID & inner_table_uuid,
    boost::intrusive_ptr<ASTStorage> inner_storage_def,
    const StorageID & time_series_storage_id,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings,
    ContextPtr context)
{
    auto create_context = Context::createCopy(context);

    auto manual_create_query = getInnerTableCreateQuery(
        inner_table_kind, inner_table_uuid, inner_storage_def,
        time_series_storage_id, time_series_columns, time_series_settings);

    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();
}


String getTimeSeriesInnerTableName(ViewTarget::Kind inner_table_kind, const StorageID & time_series_storage_id)
{
    String kind_str{magic_enum::enum_name(inner_table_kind)};
    boost::algorithm::to_lower(kind_str);
    return getTimeSeriesInnerTableName(kind_str, time_series_storage_id);
}

String getTimeSeriesInnerTableName(std::string_view inner_table_kind, const StorageID & time_series_storage_id)
{
    if (time_series_storage_id.hasUUID())
        return fmt::format(".inner_id.{}.{}", inner_table_kind, time_series_storage_id.uuid);
    else
        return fmt::format(".inner.{}.{}", inner_table_kind, time_series_storage_id.table_name);
}
}
