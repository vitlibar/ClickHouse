#include <Storages/TimeSeries/TimeSeriesInnerTablesCreator.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <DataTypes/DataTypeCustomSimpleAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace
{
    using TargetKind = ViewTarget::Kind;

    /// Makes description of the columns of an inner target table.
    ColumnsDescription getInnerTableColumnsDescription(
        TargetKind inner_table_kind, const ColumnsDescription & time_series_columns, const TimeSeriesSettings & time_series_settings)
    {
        ColumnsDescription columns;

        switch (inner_table_kind)
        {
            case TargetKind::Data:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    /// The expression for calculating the identifier of a time series can be transferred only to the "tags" inner table
                    /// (because it usually depends on columns like "metric_name" or "all_tags").
                    id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "timestamp".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Timestamp));

                /// Column "value".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Value));
                break;
            }

            case TargetKind::Tags:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    if (!time_series_settings.copy_id_default_to_tags_table)
                        id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "metric_name".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricName));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings.tags_to_columns;
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    columns.add(time_series_columns.get(column_name));
                }

                /// Column "tags".
                if (time_series_settings.store_other_tags_as_map)
                    columns.add(time_series_columns.get(TimeSeriesColumnNames::Tags));

                /// Column "all_tags".
                if (time_series_settings.create_ephemeral_all_tags_in_tags_table)
                {
                    ColumnDescription all_tags_column;
                    if (const auto * existing_column = time_series_columns.tryGet(TimeSeriesColumnNames::AllTags))
                        all_tags_column = *existing_column;
                    else
                        all_tags_column = ColumnDescription{TimeSeriesColumnNames::AllTags, std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())};
                    /// Column "all_tags" is here only to calculate the identifier of a time series for the "id" column, so it can be ephemeral.
                    all_tags_column.default_desc.kind = ColumnDefaultKind::Ephemeral;
                    if (!all_tags_column.default_desc.expression)
                    {
                        all_tags_column.default_desc.ephemeral_default = true;
                        all_tags_column.default_desc.expression = makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>(all_tags_column.type->getName()));
                    }
                    columns.add(std::move(all_tags_column));
                }

                /// Columns "min_time" and "max_time".
                if (time_series_settings.store_min_time_and_max_time)
                {
                    auto min_time_column = time_series_columns.get(TimeSeriesColumnNames::MinTime);
                    auto max_time_column = time_series_columns.get(TimeSeriesColumnNames::MaxTime);
                    if (time_series_settings.aggregate_min_time_and_max_time)
                    {
                        AggregateFunctionProperties properties;
                        auto min_function = AggregateFunctionFactory::instance().get("min", NullsAction::EMPTY, {min_time_column.type}, {}, properties);
                        auto custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(min_function, DataTypes{min_time_column.type}, Array{});
                        min_time_column.type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));

                        auto max_function = AggregateFunctionFactory::instance().get("max", NullsAction::EMPTY, {max_time_column.type}, {}, properties);
                        custom_name = std::make_unique<DataTypeCustomSimpleAggregateFunction>(max_function, DataTypes{max_time_column.type}, Array{});
                        max_time_column.type = DataTypeFactory::instance().getCustom(std::make_unique<DataTypeCustomDesc>(std::move(custom_name)));
                    }
                    columns.add(std::move(min_time_column));
                    columns.add(std::move(max_time_column));
                }

                break;
            }

            case TargetKind::Metrics:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricFamilyName));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Type));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Unit));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Help));
                break;
            }

            default:
                UNREACHABLE();
        }

        return columns;
    }

    /// Makes description of the default table engine of an inner target table.
    std::shared_ptr<ASTStorage> getDefaultEngineForInnerTable(TargetKind inner_table_kind, const TimeSeriesSettings & time_series_settings)
    {
        auto storage = std::make_shared<ASTStorage>();
        switch (inner_table_kind)
        {
            case TargetKind::Data:
            {
                storage->set(storage->engine, makeASTFunction("MergeTree"));
                storage->engine->no_empty_args = false;

                storage->set(storage->order_by,
                             makeASTFunction("tuple",
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID),
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
                break;
            }
            case TargetKind::Tags:
            {
                String engine_name;
                if (time_series_settings.aggregate_min_time_and_max_time)
                    engine_name = "AggregatingMergeTree";
                else
                    engine_name = "ReplacingMergeTree";

                storage->set(storage->engine, makeASTFunction(engine_name));
                storage->engine->no_empty_args = false;

                storage->set(storage->primary_key,
                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                ASTs order_by_list;
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
                order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID));

                if (time_series_settings.store_min_time_and_max_time && !time_series_settings.aggregate_min_time_and_max_time)
                {
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MinTime));
                    order_by_list.push_back(std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MaxTime));
                }

                auto order_by_tuple = std::make_shared<ASTFunction>();
                order_by_tuple->name = "tuple";
                auto arguments_list = std::make_shared<ASTExpressionList>();
                arguments_list->children = std::move(order_by_list);
                order_by_tuple->arguments = arguments_list;
                storage->set(storage->order_by, order_by_tuple);

                break;
            }
            case TargetKind::Metrics:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->order_by, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
                break;
            }
            default:
                UNREACHABLE();
        }
        return storage;
    }
}


StorageID TimeSeriesInnerTablesCreator::getInnerTableId(const ViewTarget & inner_table_info) const
{
    StorageID res = time_series_storage_id;
    String table_name;
    if (time_series_storage_id.hasUUID())
        res.table_name = fmt::format(".inner_id.{}.{}", toString(inner_table_info.kind), time_series_storage_id.uuid);
    else
        res.table_name = fmt::format(".inner.{}.{}", toString(inner_table_info.kind), time_series_storage_id.table_name);
    res.uuid = inner_table_info.inner_uuid;
    return res;
}

/// Generates a CREATE query for creating an inner target table.
std::shared_ptr<ASTCreateQuery> TimeSeriesInnerTablesCreator::generateCreateQueryForInnerTable(
    const ViewTarget & inner_table_info,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings) const
{
    auto create = std::make_shared<ASTCreateQuery>();

    auto inner_table_id = getInnerTableId(inner_table_info);
    create->setDatabase(inner_table_id.database_name);
    create->setTable(inner_table_id.table_name);
    create->uuid = inner_table_id.uuid;

    auto new_columns_list = std::make_shared<ASTColumns>();
    create->set(create->columns_list, new_columns_list);
    new_columns_list->set(
        new_columns_list->columns,
        InterpreterCreateQuery::formatColumns(
            getInnerTableColumnsDescription(inner_table_info.kind, time_series_columns, time_series_settings)));

    if (inner_table_info.table_engine)
    {
        create->set(create->storage, inner_table_info.table_engine->clone());

        /// Set ORDER BY if not set.
        if (!create->storage->order_by && !create->storage->primary_key && create->storage->engine && create->storage->engine->name.ends_with("MergeTree"))
        {
            auto default_engine = getDefaultEngineForInnerTable(inner_table_info.kind, time_series_settings);
            if (default_engine->order_by)
                create->storage->set(create->storage->order_by, default_engine->order_by->clone());
            if (default_engine->primary_key)
                create->storage->set(create->storage->primary_key, default_engine->primary_key->clone());
        }
    }
    else
    {
        create->set(create->storage, getDefaultEngineForInnerTable(inner_table_info.kind, time_series_settings));
    }

    return create;
}

StorageID TimeSeriesInnerTablesCreator::createInnerTable(
    const ViewTarget & inner_table_info,
    const ContextPtr & context,
    const ColumnsDescription & time_series_columns,
    const TimeSeriesSettings & time_series_settings) const
{
    /// We will make a query to create the inner target table.
    auto create_context = Context::createCopy(context);

    auto manual_create_query = generateCreateQueryForInnerTable(inner_table_info, time_series_columns, time_series_settings);

    /// Create the inner target table.
    InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
    create_interpreter.setInternal(true);
    create_interpreter.execute();

    return DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, context)->getStorageID();
}

}
