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
#include <Processors/Executors/PushingPipelineExecutor.h>


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

}


TimeSeriesSink::TargetPipeline::~TargetPipeline() = default;


std::unique_ptr<TimeSeriesSink::TargetPipeline> TimeSeriesSink::createTargetPipeline(
    ViewTarget::Kind kind, const Block & source_header)
{
    auto pipeline = std::make_unique<TargetPipeline>();

    const auto & target_table_id = time_series_storage.getTargetTableID(kind, getContext());

    auto insert_query = make_intrusive<ASTInsertQuery>();
    insert_query->table_id = target_table_id;

    auto columns_ast = make_intrusive<ASTExpressionList>();
    for (const auto & name : source_header.getNames())
        columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
    insert_query->columns = columns_ast;

    pipeline->context = Context::createCopy(getContext());
    pipeline->context->setCurrentQueryId(getContext()->getCurrentQueryId() + ":" + String{toString(kind)});

    InterpreterInsertQuery interpreter(
        insert_query,
        pipeline->context,
        /* allow_materialized= */ false,
        /* no_squash= */ false,
        /* no_destination= */ false,
        /* async_insert= */ async_insert);

    pipeline->io = interpreter.execute();
    pipeline->executor = std::make_unique<PushingPipelineExecutor>(pipeline->io.pipeline);
    pipeline->executor->start();

    /// Precompute converting actions from our source block types to the pipeline's expected types.
    const Block & expected_header = pipeline->executor->getHeader();
    auto converting_dag = ActionsDAG::makeConvertingActions(
        source_header.getColumnsWithTypeAndName(),
        expected_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name,
        pipeline->context);
    pipeline->converting_actions = std::make_shared<ExpressionActions>(
        std::move(converting_dag), ExpressionActionsSettings(pipeline->context));

    return pipeline;
}


TimeSeriesSink::TimeSeriesSink(
    StorageTimeSeries & time_series_storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_,
    Names insert_columns,
    bool async_insert_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , WithContext(context_)
    , time_series_storage(time_series_storage_)
    , time_series_settings(time_series_storage_.getStorageSettings())
    , log(getLogger("TimeSeriesSink"))
    , async_insert(async_insert_)
{
    /// Determine which target tables need pipelines based on inserted columns.
    if (insert_columns.empty())
    {
        /// All columns — create all pipelines.
        insert_tags_and_samples = true;
        insert_metrics = true;
    }
    else
    {
        for (const auto & name : insert_columns)
        {
            if (name == TimeSeriesColumnNames::TimeSeries
                || name == TimeSeriesColumnNames::MetricName
                || name == TimeSeriesColumnNames::Tags)
                insert_tags_and_samples = true;
            else if (name == TimeSeriesColumnNames::MetricFamily
                || name == TimeSeriesColumnNames::Type
                || name == TimeSeriesColumnNames::Unit
                || name == TimeSeriesColumnNames::Help)
                insert_metrics = true;
        }
    }

    const auto & settings = *time_series_settings;

    if (insert_tags_and_samples)
    {
        const auto & tags_metadata = *time_series_storage.getTargetTable(ViewTarget::Tags, getContext())->getInMemoryMetadataPtr();
        const auto & samples_metadata = *time_series_storage.getTargetTable(ViewTarget::Samples, getContext())->getInMemoryMetadataPtr();

        /// Build the tags header WITHOUT the "id" column (matches what consume() produces before ID calculation).
        Block tags_header_before_id;
        tags_header_before_id.insert(ColumnWithTypeAndName{
            castToStringType(tags_metadata.columns.get(TimeSeriesColumnNames::MetricName).type), TimeSeriesColumnNames::MetricName});

        const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            tags_header_before_id.insert(ColumnWithTypeAndName{
                castToStringType(tags_metadata.columns.get(column_name).type), column_name});
        }

        tags_header_before_id.insert(ColumnWithTypeAndName{
            castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::Tags).type), TimeSeriesColumnNames::Tags});

        if (settings[TimeSeriesSetting::use_all_tags_column_to_generate_id])
        {
            std::shared_ptr<const DataTypeMap> all_tags_type;
            if (tags_metadata.columns.has(TimeSeriesColumnNames::AllTags))
                all_tags_type = castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::AllTags).type);
            else
                all_tags_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
            tags_header_before_id.insert(ColumnWithTypeAndName{all_tags_type, TimeSeriesColumnNames::AllTags});
        }

        if (settings[TimeSeriesSetting::store_min_time_and_max_time])
        {
            tags_header_before_id.insert(ColumnWithTypeAndName{
                tags_metadata.columns.get(TimeSeriesColumnNames::MinTime).type, TimeSeriesColumnNames::MinTime});
            tags_header_before_id.insert(ColumnWithTypeAndName{
                tags_metadata.columns.get(TimeSeriesColumnNames::MaxTime).type, TimeSeriesColumnNames::MaxTime});
        }

        /// Precompute ExpressionActions for calculating the "id" column.
        DataTypePtr id_type = tags_metadata.columns.get(TimeSeriesColumnNames::ID).type;
        ColumnDescription id_column_description{TimeSeriesColumnNames::ID, id_type};
        id_column_description.default_desc.kind = ColumnDefaultKind::Default;
        id_column_description.default_desc.expression = settings[TimeSeriesSetting::id_generator].value;

        Block header_with_id;
        header_with_id.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});

        auto defaults_dag = addMissingDefaults(
            tags_header_before_id,
            header_with_id.getNamesAndTypesList(),
            ColumnsDescription{id_column_description},
            getContext());
        auto intermediate_columns = defaults_dag.getResultColumns();
        id_defaults_actions = std::make_shared<ExpressionActions>(std::move(defaults_dag));

        auto convert_dag = ActionsDAG::makeConvertingActions(
            intermediate_columns,
            header_with_id.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Position,
            getContext());
        id_convert_actions = std::make_shared<ExpressionActions>(
            std::move(convert_dag),
            ExpressionActionsSettings(getContext(), CompileExpressions::yes));

        /// Build the full tags source header WITH the "id" column (what we push to the pipeline).
        Block tags_source_header;
        tags_source_header.insert(ColumnWithTypeAndName{id_type, TimeSeriesColumnNames::ID});
        tags_source_header.insert(ColumnWithTypeAndName{
            castToStringType(tags_metadata.columns.get(TimeSeriesColumnNames::MetricName).type), TimeSeriesColumnNames::MetricName});
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            tags_source_header.insert(ColumnWithTypeAndName{
                castToStringType(tags_metadata.columns.get(column_name).type), column_name});
        }
        tags_source_header.insert(ColumnWithTypeAndName{
            castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::Tags).type), TimeSeriesColumnNames::Tags});
        if (settings[TimeSeriesSetting::store_min_time_and_max_time])
        {
            tags_source_header.insert(ColumnWithTypeAndName{
                tags_metadata.columns.get(TimeSeriesColumnNames::MinTime).type, TimeSeriesColumnNames::MinTime});
            tags_source_header.insert(ColumnWithTypeAndName{
                tags_metadata.columns.get(TimeSeriesColumnNames::MaxTime).type, TimeSeriesColumnNames::MaxTime});
        }

        tags_pipeline = createTargetPipeline(ViewTarget::Tags, tags_source_header);

        /// Build source header for samples block.
        const auto & outer_sample_block = metadata_snapshot_->getSampleBlock();
        const auto & ts_array_type = assert_cast<const DataTypeArray &>(*outer_sample_block.getByName(TimeSeriesColumnNames::TimeSeries).type);
        const auto & ts_tuple_type = assert_cast<const DataTypeTuple &>(*ts_array_type.getNestedType());

        Block samples_source_header;
        samples_source_header.insert(ColumnWithTypeAndName{
            samples_metadata.columns.get(TimeSeriesColumnNames::ID).type, TimeSeriesColumnNames::ID});
        samples_source_header.insert(ColumnWithTypeAndName{ts_tuple_type.getElement(0), TimeSeriesColumnNames::Timestamp});
        samples_source_header.insert(ColumnWithTypeAndName{ts_tuple_type.getElement(1), TimeSeriesColumnNames::Value});

        samples_pipeline = createTargetPipeline(ViewTarget::Samples, samples_source_header);
    }

    if (insert_metrics)
    {
        const auto & metrics_metadata = *time_series_storage.getTargetTable(ViewTarget::Metrics, getContext())->getInMemoryMetadataPtr();

        Block metrics_source_header;
        metrics_source_header.insert(ColumnWithTypeAndName{
            castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::MetricFamilyName).type), TimeSeriesColumnNames::MetricFamilyName});
        metrics_source_header.insert(ColumnWithTypeAndName{
            castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Type).type), TimeSeriesColumnNames::Type});
        metrics_source_header.insert(ColumnWithTypeAndName{
            castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Unit).type), TimeSeriesColumnNames::Unit});
        metrics_source_header.insert(ColumnWithTypeAndName{
            castToStringType(metrics_metadata.columns.get(TimeSeriesColumnNames::Help).type), TimeSeriesColumnNames::Help});

        metrics_pipeline = createTargetPipeline(ViewTarget::Metrics, metrics_source_header);
    }
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

    /// ========== Build and push Tags + Samples blocks ==========

    if (insert_tags_and_samples)
    {
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
        const auto & settings = *time_series_settings;

        /// --- Prepare columns for the Tags block ---

        DataTypePtr metric_name_type = castToStringType(tags_metadata.columns.get(TimeSeriesColumnNames::MetricName).type);
        auto metric_name_column = metric_name_type->createColumn();
        metric_name_column->reserve(num_rows);

        std::vector<IColumn *> columns_to_fill_in_tags_table;

        std::unordered_map<std::string_view, std::pair<MutableColumnPtr, DataTypePtr>> columns_by_tag_name;
        const Map & tags_to_columns = settings[TimeSeriesSetting::tags_to_columns];
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

        auto tags_map_type = castToStringMapType(tags_metadata.columns.get(TimeSeriesColumnNames::Tags).type);
        auto residual_tags_names = tags_map_type->getKeyType()->createColumn();
        residual_tags_names->reserve(num_rows);
        auto residual_tags_values = tags_map_type->getValueType()->createColumn();
        residual_tags_values->reserve(num_rows);
        auto residual_tags_offsets = ColumnVector<IColumn::Offset>::create();
        residual_tags_offsets->reserve(num_rows);

        MutableColumnPtr all_tags_names;
        MutableColumnPtr all_tags_values;
        ColumnVector<IColumn::Offset>::MutablePtr all_tags_offsets;
        std::shared_ptr<const DataTypeMap> all_tags_map_type;
        bool use_all_tags = settings[TimeSeriesSetting::use_all_tags_column_to_generate_id];
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

        MutableColumnPtr min_time_column;
        MutableColumnPtr max_time_column;
        DataTypePtr min_time_type;
        DataTypePtr max_time_type;
        bool store_min_max_time = settings[TimeSeriesSetting::store_min_time_and_max_time];
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

        /// Access input tags map internals.
        const auto & input_tags_nested = input_tags_col.getNestedColumn();
        const auto & input_tags_offsets = input_tags_nested.getOffsets();
        const auto & input_tags_tuple = assert_cast<const ColumnTuple &>(input_tags_nested.getData());
        const auto & input_tags_keys = input_tags_tuple.getColumn(0);
        const auto & input_tags_values_col = input_tags_tuple.getColumn(1);

        /// --- Prepare columns for the Samples block ---

        DataTypePtr samples_id_type = samples_metadata.columns.get(TimeSeriesColumnNames::ID).type;
        auto samples_id_column = samples_id_type->createColumn();
        samples_id_column->reserve(num_samples);

        auto timestamp_column = timestamp_type->createColumn();
        timestamp_column->reserve(num_samples);

        auto value_column = value_type->createColumn();
        value_column->reserve(num_samples);

        /// --- Process rows ---

        std::vector<std::pair<std::string_view, std::string_view>> sorted_tags;

        size_t current_row_in_tags = 0;
        for (size_t row = 0; row < num_rows; ++row)
        {
            size_t ts_start = (row == 0) ? 0 : ts_offsets[row - 1];
            size_t ts_end = ts_offsets[row];
            size_t array_size = ts_end - ts_start;

            if (array_size == 0)
                continue;

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

            /// Add __name__ from the metric_name column if not already present.
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

            residual_tags_offsets->insertValue(residual_tags_names->size());
            if (use_all_tags)
                all_tags_offsets->insertValue(all_tags_names->size());

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

            for (auto * column : columns_to_fill_in_tags_table)
            {
                if (column->size() == current_row_in_tags)
                    column->insertDefault();
            }

            timestamp_column->insertRangeFrom(ts_timestamps, ts_start, array_size);
            value_column->insertRangeFrom(ts_values, ts_start, array_size);

            ++current_row_in_tags;
        }

        if (current_row_in_tags > 0)
        {
            /// Assemble Tags block.
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

            /// Calculate IDs using precomputed ExpressionActions.
            Block id_block = tags_block;
            id_defaults_actions->execute(id_block);
            id_convert_actions->execute(id_block);
            auto id_column = id_block.getByName(TimeSeriesColumnNames::ID).column;
            DataTypePtr id_type = id_block.getByName(TimeSeriesColumnNames::ID).type;
            tags_block.insert(0, ColumnWithTypeAndName{id_column, id_type, TimeSeriesColumnNames::ID});

            if (tags_block.has(TimeSeriesColumnNames::AllTags))
                tags_block.erase(TimeSeriesColumnNames::AllTags);

            /// Fill Samples ID column.
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

            /// Assemble Samples block.
            Block samples_block;
            samples_block.insert(ColumnWithTypeAndName{std::move(samples_id_column), samples_id_type, TimeSeriesColumnNames::ID});
            samples_block.insert(ColumnWithTypeAndName{std::move(timestamp_column), timestamp_type, TimeSeriesColumnNames::Timestamp});
            samples_block.insert(ColumnWithTypeAndName{std::move(value_column), value_type, TimeSeriesColumnNames::Value});

            /// Push to persistent pipelines.
            tags_pipeline->converting_actions->execute(tags_block);
            tags_pipeline->executor->push(std::move(tags_block));

            samples_pipeline->converting_actions->execute(samples_block);
            samples_pipeline->executor->push(std::move(samples_block));
        }
    }

    /// ========== Build and push Metrics block ==========

    if (insert_metrics)
    {
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

        if (metric_family_name_column->size() > 0)
        {
            Block metrics_block;
            metrics_block.insert(ColumnWithTypeAndName{std::move(metric_family_name_column), metric_family_name_type, TimeSeriesColumnNames::MetricFamilyName});
            metrics_block.insert(ColumnWithTypeAndName{std::move(type_column_out), type_type, TimeSeriesColumnNames::Type});
            metrics_block.insert(ColumnWithTypeAndName{std::move(unit_column_out), unit_type, TimeSeriesColumnNames::Unit});
            metrics_block.insert(ColumnWithTypeAndName{std::move(help_column_out), help_type, TimeSeriesColumnNames::Help});

            metrics_pipeline->converting_actions->execute(metrics_block);
            metrics_pipeline->executor->push(std::move(metrics_block));
        }
    }
}


void TimeSeriesSink::onFinish()
{
    if (tags_pipeline)
        tags_pipeline->executor->finish();
    if (samples_pipeline)
        samples_pipeline->executor->finish();
    if (metrics_pipeline)
        metrics_pipeline->executor->finish();
}

}
