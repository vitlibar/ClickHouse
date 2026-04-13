#include <Storages/TimeSeries/TimeSeriesSink.h>

#include <algorithm>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/logger_useful.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/addMissingDefaults.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Sources/BlocksSource.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/Pipe.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsASTFunction id_generator;
    extern const TimeSeriesSettingsDataType id_type;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool use_all_tags_column_to_generate_id;
}


namespace
{
    /// Returns the type unchanged if it is String or LowCardinality(String), otherwise returns DataTypeString.
    DataTypePtr castToStringType(const DataTypePtr & type)
    {
        if (isString(removeLowCardinality(type)))
            return type;
        return std::make_shared<DataTypeString>();
    }

    /// Returns the type unchanged if it is Map with string-like keys and values, otherwise returns Map(String, String).
    std::shared_ptr<const DataTypeMap> castToStringMapType(const DataTypePtr & type)
    {
        if (const auto * map_type = typeid_cast<const DataTypeMap *>(type.get());
            map_type
            && isString(removeLowCardinality(map_type->getKeyType()))
            && isString(removeLowCardinality(map_type->getValueType())))
            return std::static_pointer_cast<const DataTypeMap>(type);
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    /// Calculates the identifier of each time series in "tags_block" using the default expression for the "id" column,
    /// and returns column "id" with the results.
    ColumnPtr calculateId(const ContextPtr & context, const TimeSeriesSettings & time_series_settings, const Block & tags_block)
    {
        DataTypePtr id_type = time_series_settings[TimeSeriesSetting::id_type];
        ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
        id_column_description.default_desc.kind = ColumnDefaultKind::Default;
        id_column_description.default_desc.expression = time_series_settings[TimeSeriesSetting::id_generator].value;

        auto blocks = std::make_shared<Blocks>();
        blocks->push_back(tags_block);

        auto header = std::make_shared<const Block>(tags_block.cloneEmpty());
        auto pipe = Pipe(std::make_shared<BlocksSource>(blocks, header));

        Block header_with_id;
        const auto & id_name = id_column_description.name;
        header_with_id.insert(ColumnWithTypeAndName{id_type, id_name});

        auto adding_missing_defaults_dag = addMissingDefaults(
            pipe.getHeader(),
            header_with_id.getNamesAndTypesList(),
            ColumnsDescription{id_column_description},
            context);

        auto adding_missing_defaults_actions = std::make_shared<ExpressionActions>(std::move(adding_missing_defaults_dag));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, adding_missing_defaults_actions);
        });

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            pipe.getHeader().getColumnsWithTypeAndName(),
            header_with_id.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            context);
        auto actions = std::make_shared<ExpressionActions>(
            std::move(convert_actions_dag),
            ExpressionActionsSettings(context, CompileExpressions::yes));
        pipe.addSimpleTransform([&](const SharedHeader & stream_header)
        {
            return std::make_shared<ExpressionTransform>(stream_header, actions);
        });

        QueryPipeline pipeline{std::move(pipe)};
        PullingPipelineExecutor executor{pipeline};

        MutableColumnPtr id_column;

        Block block_from_executor;
        while (executor.pull(block_from_executor))
        {
            if (!block_from_executor.empty())
            {
                MutableColumnPtr id_column_part = block_from_executor.getByName(id_name).column->assumeMutable();
                if (id_column)
                    id_column->insertRangeFrom(*id_column_part, 0, id_column_part->size());
                else
                    id_column = std::move(id_column_part);
            }
        }

        if (!id_column)
            id_column = id_type->createColumn();

        return std::move(id_column);
    }

    struct BlocksToInsert
    {
        std::vector<std::pair<ViewTarget::Kind, Block>> blocks;
    };

    /// Inserts blocks to target tables.
    void insertToTargetTables(BlocksToInsert && blocks, StorageTimeSeries & time_series_storage, ContextPtr context, Poco::Logger * log)
    {
        auto time_series_storage_id = time_series_storage.getStorageID();

        for (auto & [table_kind, block] : blocks.blocks)
        {
            if (!block.empty())
            {
                const auto & target_table_id = time_series_storage.getTargetTableID(table_kind, context);

                LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                         time_series_storage_id.getNameForLogs(), block.rows(), toString(table_kind));

                auto insert_query = make_intrusive<ASTInsertQuery>();
                insert_query->table_id = target_table_id;

                auto columns_ast = make_intrusive<ASTExpressionList>();
                for (const auto & name : block.getNames())
                    columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
                insert_query->columns = columns_ast;

                ContextMutablePtr insert_context = Context::createCopy(context);
                insert_context->setCurrentQueryId(context->getCurrentQueryId() + ":" + String{toString(table_kind)});

                LOG_TEST(log, "{}: Executing query: {}", time_series_storage_id.getNameForLogs(), insert_query->formatForLogging());

                InterpreterInsertQuery interpreter(
                    insert_query,
                    insert_context,
                    /* allow_materialized= */ false,
                    /* no_squash= */ false,
                    /* no_destination= */ false,
                    /* async_insert= */ false);

                BlockIO io = interpreter.execute();
                PushingPipelineExecutor executor(io.pipeline);

                executor.start();

                /// Convert block columns to match what the pipeline expects.
                const Block & expected_header = executor.getHeader();
                auto converting_dag = ActionsDAG::makeConvertingActions(
                    block.getColumnsWithTypeAndName(),
                    expected_header.getColumnsWithTypeAndName(),
                    ActionsDAG::MatchColumnsMode::Name,
                    insert_context);
                auto converting_actions = std::make_shared<ExpressionActions>(
                    std::move(converting_dag), ExpressionActionsSettings(insert_context));
                converting_actions->execute(block);

                executor.push(std::move(block));
                executor.finish();
            }
        }
    }
}


TimeSeriesSink::TimeSeriesSink(
    StorageTimeSeries & time_series_storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , WithContext(context_)
    , time_series_storage(time_series_storage_)
    , time_series_settings(time_series_storage_.getStorageSettings())
    , log(getLogger("TimeSeriesSink"))
{
}


void TimeSeriesSink::consume(Chunk & chunk)
{
    size_t num_rows = chunk.getNumRows();
    if (!num_rows)
        return;

    Block block = getHeader().cloneWithColumns(chunk.getColumns());

    /// Extract input columns.
    const auto & time_series_col = assert_cast<const ColumnArray &>(*block.getByName(TimeSeriesColumnNames::TimeSeries).column);
    const auto & input_metric_name_col = *block.getByName(TimeSeriesColumnNames::MetricName).column;
    const auto & input_tags_col = assert_cast<const ColumnMap &>(*block.getByName(TimeSeriesColumnNames::Tags).column);
    const auto & input_metric_family_col = *block.getByName(TimeSeriesColumnNames::MetricFamily).column;
    const auto & input_type_col = *block.getByName(TimeSeriesColumnNames::Type).column;
    const auto & input_unit_col = *block.getByName(TimeSeriesColumnNames::Unit).column;
    const auto & input_help_col = *block.getByName(TimeSeriesColumnNames::Help).column;

    /// Access time_series array internals: Array(Tuple(timestamp, value)).
    const auto & ts_offsets = time_series_col.getOffsets();
    const auto & ts_tuple = assert_cast<const ColumnTuple &>(time_series_col.getData());
    const auto & ts_timestamps = ts_tuple.getColumn(0);
    const auto & ts_values = ts_tuple.getColumn(1);

    /// Get timestamp/value types from the time_series array's inner tuple.
    const auto & ts_array_type = assert_cast<const DataTypeArray &>(*block.getByName(TimeSeriesColumnNames::TimeSeries).type);
    const auto & ts_tuple_type = assert_cast<const DataTypeTuple &>(*ts_array_type.getNestedType());
    DataTypePtr timestamp_type = ts_tuple_type.getElement(0);
    DataTypePtr value_type = ts_tuple_type.getElement(1);

    size_t num_samples = ts_offsets.empty() ? 0 : ts_offsets.back();

    /// Get target table metadata.
    const auto & tags_metadata = *time_series_storage.getTargetTable(ViewTarget::Tags, getContext())->getInMemoryMetadataPtr();
    const auto & samples_metadata = *time_series_storage.getTargetTable(ViewTarget::Samples, getContext())->getInMemoryMetadataPtr();

    /// ========== Prepare columns for the Tags block ==========

    /// Column "metric_name".
    DataTypePtr metric_name_type = castToStringType(tags_metadata.columns.get(TimeSeriesColumnNames::MetricName).type);
    auto metric_name_column = metric_name_type->createColumn();
    metric_name_column->reserve(num_rows);

    /// Columns we should check explicitly that they're filled after filling each row.
    std::vector<IColumn *> columns_to_fill_in_tags_table;

    /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
    std::unordered_map<std::string_view, std::pair<MutableColumnPtr, DataTypePtr>> columns_by_tag_name;
    const Map & tags_to_columns = (*time_series_settings)[TimeSeriesSetting::tags_to_columns];
    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        DataTypePtr column_type = castToStringType(tags_metadata.columns.get(column_name).type);
        auto column = column_type->createColumn();
        column->reserve(num_rows);
        columns_to_fill_in_tags_table.emplace_back(column.get());
        columns_by_tag_name[tag_name] = {std::move(column), column_type};
    }

    /// Column "tags" (residual map: input tags minus tags_to_columns entries).
    auto tags_map_type = castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::Tags).type);
    auto residual_tags_names = tags_map_type->getKeyType()->createColumn();
    residual_tags_names->reserve(num_rows);
    auto residual_tags_values = tags_map_type->getValueType()->createColumn();
    residual_tags_values->reserve(num_rows);
    auto residual_tags_offsets = ColumnVector<IColumn::Offset>::create();
    residual_tags_offsets->reserve(num_rows);

    /// Column "all_tags" (if needed for ID generation).
    MutableColumnPtr all_tags_names;
    MutableColumnPtr all_tags_values;
    ColumnVector<IColumn::Offset>::MutablePtr all_tags_offsets;
    std::shared_ptr<const DataTypeMap> all_tags_map_type;
    bool use_all_tags = (*time_series_settings)[TimeSeriesSetting::use_all_tags_column_to_generate_id];
    if (use_all_tags)
    {
        if (tags_metadata.columns.has(TimeSeriesColumnNames::AllTags))
            all_tags_map_type = castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::AllTags).type);
        else
            all_tags_map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
        all_tags_names = all_tags_map_type->getKeyType()->createColumn();
        all_tags_names->reserve(num_rows);
        all_tags_values = all_tags_map_type->getValueType()->createColumn();
        all_tags_values->reserve(num_rows);
        all_tags_offsets = ColumnVector<IColumn::Offset>::create();
        all_tags_offsets->reserve(num_rows);
    }

    /// Columns "min_time" and "max_time".
    MutableColumnPtr min_time_column;
    MutableColumnPtr max_time_column;
    DataTypePtr min_time_type;
    DataTypePtr max_time_type;
    bool store_min_max_time = (*time_series_settings)[TimeSeriesSetting::store_min_time_and_max_time];
    if (store_min_max_time)
    {
        min_time_type = tags_metadata.columns.get(TimeSeriesColumnNames::MinTime).type;
        max_time_type = tags_metadata.columns.get(TimeSeriesColumnNames::MaxTime).type;
        min_time_column = min_time_type->createColumn();
        max_time_column = max_time_type->createColumn();
        min_time_column->reserve(num_rows);
        max_time_column->reserve(num_rows);
        columns_to_fill_in_tags_table.emplace_back(min_time_column.get());
        columns_to_fill_in_tags_table.emplace_back(max_time_column.get());
    }

    /// Access input tags map internals: Map is ColumnArray(ColumnTuple(keys, values)).
    const auto & input_tags_nested = input_tags_col.getNestedColumn();
    const auto & input_tags_offsets = input_tags_nested.getOffsets();
    const auto & input_tags_tuple = assert_cast<const ColumnTuple &>(input_tags_nested.getData());
    const auto & input_tags_keys = input_tags_tuple.getColumn(0);
    const auto & input_tags_values_col = input_tags_tuple.getColumn(1);

    /// ========== Prepare columns for the Samples block ==========

    DataTypePtr samples_id_type = samples_metadata.columns.get(TimeSeriesColumnNames::ID).type;
    auto samples_id_column = samples_id_type->createColumn();
    samples_id_column->reserve(num_samples);

    auto timestamp_column = timestamp_type->createColumn();
    timestamp_column->reserve(num_samples);

    auto value_column = value_type->createColumn();
    value_column->reserve(num_samples);

    /// ========== Process rows ==========

    std::vector<std::pair<std::string_view, std::string_view>> sorted_tags;

    size_t current_row_in_tags = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        size_t ts_start = (row == 0) ? 0 : ts_offsets[row - 1];
        size_t ts_end = ts_offsets[row];
        size_t array_size = ts_end - ts_start;

        /// Skip rows with empty time_series array (no samples to insert).
        if (array_size == 0)
            continue;

        /// --- Tags ---

        /// Collect all tags from the input tags map for this row.
        sorted_tags.clear();
        size_t map_start = (row == 0) ? 0 : input_tags_offsets[row - 1];
        size_t map_end = input_tags_offsets[row];
        sorted_tags.reserve(map_end - map_start + 1);

        for (size_t j = map_start; j < map_end; ++j)
        {
            std::string_view key = input_tags_keys.getDataAt(j);
            std::string_view value = input_tags_values_col.getDataAt(j);
            sorted_tags.emplace_back(key, value);
        }

        /// Add __name__ from the metric_name column if not already present in tags.
        std::string_view metric_name_sv = input_metric_name_col.getDataAt(row);
        if (!metric_name_sv.empty())
        {
            bool has_name = std::any_of(sorted_tags.begin(), sorted_tags.end(),
                [](const auto & tag) { return tag.first == TimeSeriesTagNames::MetricName; });
            if (!has_name)
                sorted_tags.emplace_back(TimeSeriesTagNames::MetricName, metric_name_sv);
        }

        /// Sort tags by name, remove exact duplicates and tags with empty values.
        std::sort(sorted_tags.begin(), sorted_tags.end(),
            [](const auto & left, const auto & right) { return left.first < right.first; });
        sorted_tags.erase(std::unique(sorted_tags.begin(), sorted_tags.end()), sorted_tags.end());
        std::erase_if(sorted_tags, [](const auto & x) { return x.second.empty(); });

        /// Process sorted tags: split into metric_name, specific tag columns, residual tags, and all_tags.
        for (const auto & [tag_name, tag_value] : sorted_tags)
        {
            if (tag_name == TimeSeriesTagNames::MetricName)
            {
                metric_name_column->insertData(tag_value.data(), tag_value.size());
            }
            else
            {
                if (use_all_tags)
                {
                    all_tags_names->insertData(tag_name.data(), tag_name.size());
                    all_tags_values->insertData(tag_value.data(), tag_value.size());
                }

                auto it = columns_by_tag_name.find(tag_name);
                if (it != columns_by_tag_name.end())
                {
                    it->second.first->insertData(tag_value.data(), tag_value.size());
                }
                else
                {
                    residual_tags_names->insertData(tag_name.data(), tag_name.size());
                    residual_tags_values->insertData(tag_value.data(), tag_value.size());
                }
            }
        }

        /// Close offsets for this row.
        residual_tags_offsets->insertValue(residual_tags_names->size());
        if (use_all_tags)
            all_tags_offsets->insertValue(all_tags_names->size());

        /// Compute min_time/max_time from the time_series array.
        if (store_min_max_time)
        {
            Field min_ts, max_ts;
            ts_timestamps.get(ts_start, min_ts);
            max_ts = min_ts;
            for (size_t j = ts_start + 1; j < ts_end; ++j)
            {
                Field ts;
                ts_timestamps.get(j, ts);
                if (ts < min_ts)
                    min_ts = ts;
                if (ts > max_ts)
                    max_ts = ts;
            }
            min_time_column->insert(min_ts);
            max_time_column->insert(max_ts);
        }

        /// Fill defaults for tag columns that were not populated this row.
        for (auto * column : columns_to_fill_in_tags_table)
        {
            if (column->size() == current_row_in_tags)
                column->insertDefault();
        }

        /// --- Samples ---

        /// Copy timestamps and values from the array.
        timestamp_column->insertRangeFrom(ts_timestamps, ts_start, array_size);
        value_column->insertRangeFrom(ts_values, ts_start, array_size);

        ++current_row_in_tags;
    }

    /// All rows had empty time_series arrays - nothing to insert.
    if (current_row_in_tags == 0)
        return;

    /// ========== Assemble Tags block ==========

    Block tags_block;
    tags_block.insert(ColumnWithTypeAndName{std::move(metric_name_column), metric_name_type, TimeSeriesColumnNames::MetricName});

    for (const auto & tag_name_and_column_name : tags_to_columns)
    {
        const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
        const auto & tag_name = tuple.at(0).safeGet<String>();
        const auto & column_name = tuple.at(1).safeGet<String>();
        auto & [column, column_type] = columns_by_tag_name.at(tag_name);
        tags_block.insert(ColumnWithTypeAndName{std::move(column), column_type, column_name});
    }

    MutableColumns tags_tuple_cols;
    tags_tuple_cols.push_back(std::move(residual_tags_names));
    tags_tuple_cols.push_back(std::move(residual_tags_values));
    auto residual_tags_column = ColumnMap::create(
        ColumnArray::create(ColumnTuple::create(std::move(tags_tuple_cols)), std::move(residual_tags_offsets)));
    tags_block.insert(ColumnWithTypeAndName{std::move(residual_tags_column), tags_map_type, TimeSeriesColumnNames::Tags});

    if (all_tags_names)
    {
        MutableColumns all_tags_tuple_cols;
        all_tags_tuple_cols.push_back(std::move(all_tags_names));
        all_tags_tuple_cols.push_back(std::move(all_tags_values));
        auto all_tags_column = ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(std::move(all_tags_tuple_cols)), std::move(all_tags_offsets)));
        tags_block.insert(ColumnWithTypeAndName{std::move(all_tags_column), all_tags_map_type, TimeSeriesColumnNames::AllTags});
    }

    if (min_time_column)
    {
        tags_block.insert(ColumnWithTypeAndName{std::move(min_time_column), min_time_type, TimeSeriesColumnNames::MinTime});
        tags_block.insert(ColumnWithTypeAndName{std::move(max_time_column), max_time_type, TimeSeriesColumnNames::MaxTime});
    }

    /// Calculate an identifier for each time series and add the result column to the tags block.
    DataTypePtr id_type = tags_metadata.columns.get(TimeSeriesColumnNames::ID).type;
    auto id_column = calculateId(getContext(), *time_series_settings, tags_block);
    tags_block.insert(0, ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});

    /// The "all_tags" column is ephemeral - remove it after ID calculation.
    if (tags_block.has(TimeSeriesColumnNames::AllTags))
        tags_block.erase(TimeSeriesColumnNames::AllTags);

    /// ========== Fill Samples ID column ==========

    size_t tags_row = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        size_t ts_start = (row == 0) ? 0 : ts_offsets[row - 1];
        size_t ts_end = ts_offsets[row];
        size_t array_size = ts_end - ts_start;

        if (array_size == 0)
            continue;

        samples_id_column->insertManyFrom(*id_column, tags_row, array_size);
        ++tags_row;
    }

    /// ========== Assemble Samples block ==========

    Block samples_block;
    samples_block.insert(ColumnWithTypeAndName{std::move(samples_id_column), samples_id_type, TimeSeriesColumnNames::ID});
    samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
    samples_block.insert(ColumnWithTypeAndName{std::move(value_column), value_type, TimeSeriesColumnNames::Value});

    /// ========== Build Metrics block ==========

    const auto & metrics_metadata = *time_series_storage.getTargetTable(ViewTarget::Metrics, getContext())->getInMemoryMetadataPtr();
    DataTypePtr metric_family_name_type = castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::MetricFamilyName).type);
    DataTypePtr type_type = castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Type).type);
    DataTypePtr unit_type = castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Unit).type);
    DataTypePtr help_type = castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Help).type);

    auto metric_family_name_column = metric_family_name_type->createColumn();
    auto type_column_out = type_type->createColumn();
    auto unit_column_out = unit_type->createColumn();
    auto help_column_out = help_type->createColumn();

    for (size_t row = 0; row < num_rows; ++row)
    {
        /// Skip rows where metric_family is empty.
        std::string_view metric_family = input_metric_family_col.getDataAt(row);
        if (metric_family.empty())
            continue;

        std::string_view type_sv = input_type_col.getDataAt(row);
        std::string_view unit_sv = input_unit_col.getDataAt(row);
        std::string_view help_sv = input_help_col.getDataAt(row);

        metric_family_name_column->insertData(metric_family.data(), metric_family.size());
        type_column_out->insertData(type_sv.data(), type_sv.size());
        unit_column_out->insertData(unit_sv.data(), unit_sv.size());
        help_column_out->insertData(help_sv.data(), help_sv.size());
    }

    Block metrics_block;
    if (metric_family_name_column->size() > 0)
    {
        metrics_block.insert(ColumnWithTypeAndName{std::move(metric_family_name_column), metric_family_name_type, TimeSeriesColumnNames::MetricFamilyName});
        metrics_block.insert(ColumnWithTypeAndName{std::move(type_column_out), type_type, TimeSeriesColumnNames::Type});
        metrics_block.insert(ColumnWithTypeAndName{std::move(unit_column_out), unit_type, TimeSeriesColumnNames::Unit});
        metrics_block.insert(ColumnWithTypeAndName{std::move(help_column_out), help_type, TimeSeriesColumnNames::Help});
    }

    /// ========== Insert to target tables ==========

    BlocksToInsert blocks;

    /// Tags table should be inserted first (before samples, to ensure ID consistency).
    blocks.blocks.emplace_back(ViewTarget::Tags, std::move(tags_block));
    blocks.blocks.emplace_back(ViewTarget::Samples, std::move(samples_block));
    if (!metrics_block.empty())
        blocks.blocks.emplace_back(ViewTarget::Metrics, std::move(metrics_block));

    insertToTargetTables(std::move(blocks), time_series_storage, getContext(), log.get());
}

}
