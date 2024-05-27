#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include "config.h"
#if USE_PROMETHEUS_PROTOBUFS

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesIDCalculator.h>
#include <Storages/TimeSeries/TimeSeriesLabelNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
}


namespace
{
    /// Checks that a specified set of labels is correct - i.e. the labels are sorted by name, no duplications,
    /// and there is the "__name__" label containing the metric name.
    void checkLabels(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels)
    {
        bool metric_name_found = false;
        for (int i = 0; i != labels.size(); ++i)
        {
            const auto & label = labels[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();

            if (label_name.empty())
                throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Label name should not be empty");
            if (label_value.empty())
                continue; /// Empty label value is treated like the label doesn't exist.

            if (label_name == TimeSeriesLabelNames::kMetricName)
                metric_name_found = true;

            if (i)
            {
                /// Check that labels are sorted.
                const auto & previous_label_name = labels[i - 1].name();
                if (label_name <= previous_label_name)
                {
                    if (label_name == previous_label_name)
                        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Found duplicate label {}", label_name);
                    else
                        throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Label names are not sorted in lexicographical order ({} > {})",
                                        previous_label_name, label_name);
                }
            }
        }

        if (!metric_name_found)
            throw Exception(ErrorCodes::ILLEGAL_TIME_SERIES_TAGS, "Metric name (label __name__) not found");
    }

    /// Converts a timestamp in milliseconds to a DateTime64 with a specified scale.
    DateTime64 timestampToDateTime64(UInt64 timestamp_ms, UInt32 scale)
    {
        if (scale == 3)
            return timestamp_ms;
        else if (scale > 3)
            return timestamp_ms * DecimalUtils::scaleMultiplier<DateTime64>(scale - 3);
        else
            return timestamp_ms / DecimalUtils::scaleMultiplier<DateTime64>(3 - scale);
    }

    struct BlocksToInsert
    {
        std::vector<std::pair<TargetTableKind, Block>> blocks;
    };

    /// Converts time series in the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
                            const StorageID & time_series_storage_id,
                            const TimeSeriesSettings & time_series_settings,
                            ITimeSeriesIDCalculator & id_calculator,
                            const StorageInMemoryMetadata & data_table_metadata,
                            const StorageInMemoryMetadata & tags_table_metadata)
    {
        size_t num_tags_rows = time_series.size();

        size_t num_data_rows = 0;
        for (const auto & element : time_series)
            num_data_rows += element.samples_size();

        if (!num_data_rows)
            return {}; /// Nothing to insert into target tables.

        /// Column types must be extracted from the target tables' metadata.
        const auto & data_table_description = data_table_metadata.columns;
        const auto & tags_table_description = tags_table_metadata.columns;

        GetColumnsOptions insertable_columns = static_cast<GetColumnsOptions::Kind>(GetColumnsOptions::Ordinary | GetColumnsOptions::Ephemeral);
        auto id_type               = data_table_description.getColumn(insertable_columns, TimeSeriesColumnNames::kID).type;
        auto timestamp_type        = data_table_description.getColumn(insertable_columns, TimeSeriesColumnNames::kTimestamp).type;
        auto value_type            = data_table_description.getColumn(insertable_columns, TimeSeriesColumnNames::kValue).type;
        auto id_type_in_tags_table = tags_table_description.getColumn(insertable_columns, TimeSeriesColumnNames::kID).type;
        auto string_type = std::make_shared<DataTypeString>();

        /// Check data types.
        auto id_type_index = id_type->getTypeId();
        if (!id_type->equals(*id_type_in_tags_table))
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The '{}' column in the data table is expected to have the same data type as the '{}' column in the tags table, now they're different ({} != {})",
                            time_series_storage_id.getNameForLogs(), TimeSeriesColumnNames::kID, TimeSeriesColumnNames::kID,
                            id_type->getName(), id_type_in_tags_table->getName());
        }

        if (timestamp_type->getTypeId() != TypeIndex::DateTime64)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The '{}' column in the data table has wrong data type {}, DateTime64(s) is expected where s >= 3",
                            time_series_storage_id.getNameForLogs(), TimeSeriesColumnNames::kTimestamp, timestamp_type->getName());
        }

        auto timestamp_scale = typeid_cast<const DataTypeDateTime64 &>(*timestamp_type).getScale();
        if (timestamp_scale < 3)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The '{}' column in the data table has wrong data type {}, DateTime64(s) is expected where s >= 3",
                            time_series_storage_id.getNameForLogs(), TimeSeriesColumnNames::kTimestamp, timestamp_type->getName());
        }

        /// We're going to prepare two blocks - one for the data table, and one for the tags table.
        Block data_block, tags_block;

        auto add_column_to_data_block = [&](const String & column_name_, const DataTypePtr & type_) -> IColumn &
        {
            auto column = type_->createColumn();
            column->reserve(num_data_rows);
            auto * ptr = column.get();
            data_block.insert(ColumnWithTypeAndName{std::move(column), type_, column_name_});
            return *ptr;
        };

        auto add_column_to_tags_block = [&](const String & column_name_, const DataTypePtr & type_) -> IColumn &
        {
            auto column = type_->createColumn();
            column->reserve(num_tags_rows);
            auto * ptr = column.get();
            tags_block.insert(ColumnWithTypeAndName{std::move(column), type_, column_name_});
            return *ptr;
        };

        /// Create columns.
        auto & id_column_in_data_table = add_column_to_data_block(TimeSeriesColumnNames::kID,        id_type);
        auto & timestamp_column        = add_column_to_data_block(TimeSeriesColumnNames::kTimestamp, timestamp_type);
        auto & value_column            = add_column_to_data_block(TimeSeriesColumnNames::kValue,     value_type);
        auto & id_column_in_tags_table = add_column_to_tags_block(TimeSeriesColumnNames::kID,        id_type);

        std::vector<IColumn *> columns_to_fill_in_tags_table; /// Columns we should check explicitly that they're filled after filling each row.

        std::unordered_map<String, IColumn *> column_by_label_name;

        auto add_column_for_label = [&](const String & column_name, const String & label_name)
        {
            auto & column = add_column_to_tags_block(column_name, string_type);
            column_by_label_name[label_name] = &column;
            columns_to_fill_in_tags_table.emplace_back(&column);
        };

        add_column_for_label(TimeSeriesColumnNames::kMetricName, TimeSeriesLabelNames::kMetricName);

        const Map & tags_to_columns = time_series_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            add_column_for_label(column_name, tag_name);
        }

        /// The "other_tags" column gets tags which don't have assigned columns.
        auto other_tags_type = std::make_shared<DataTypeMap>(string_type, string_type);
        auto & other_tags_column = typeid_cast<ColumnMap &>(add_column_to_tags_block(TimeSeriesColumnNames::kTags, other_tags_type));
        auto & other_tags_names = typeid_cast<ColumnString &>(other_tags_column.getNestedData().getColumn(0));
        auto & other_tags_values = typeid_cast<ColumnString &>(other_tags_column.getNestedData().getColumn(1));
        auto & other_tags_offsets = typeid_cast<ColumnArray::ColumnOffsets &>(other_tags_column.getNestedColumn().getOffsetsColumn());
        
        /// Fill columns.
        std::vector<UInt8> labels_written;

        for (size_t i = 0; i != static_cast<size_t>(time_series.size()); ++i)
        {
            const auto & element = time_series[static_cast<int>(i)];

            const auto & labels = element.labels();
            checkLabels(labels);
            auto id = id_calculator.calculateID(labels, id_type_index);

            /// Fill columns in the tags table.
            labels_written.clear();

            for (size_t j = 0; j != static_cast<size_t>(labels.size()); ++j)
            {
                const auto & label = labels[static_cast<int>(j)];
                const auto & label_name = label.name();
                const auto & label_value = label.value();

                auto it = column_by_label_name.find(label_name);
                if (it != column_by_label_name.end())
                {
                    auto * column = it->second;
                    column->insertData(label_value.data(), label_value.length());
                    if (labels_written.size() < j + 1)
                        labels_written.resize(j + 1, false);
                    labels_written[j] = true;
                }
            }

            for (int j = 0; j != labels.size(); ++j)
            {
                if ((j >= static_cast<int>(labels_written.size())) || !labels_written[j])
                {
                    const auto & label = labels[j];
                    const auto & label_name = label.name();
                    const auto & label_value = label.value();
                    other_tags_names.insertData(label_name.data(), label_name.length());
                    other_tags_values.insertData(label_value.data(), label_value.length());
                }
            }

            other_tags_offsets.insertValue(other_tags_names.size());
            id_column_in_tags_table.insert(id);

            size_t current_num_tags_rows = i + 1;
            for (auto * column : columns_to_fill_in_tags_table)
            {
                if (column->size() < current_num_tags_rows)
                    column->insertDefault();
            }

            /// Fill columns in the data table.
            for (const auto & sample : element.samples())
            {
                id_column_in_data_table.insert(id);
                timestamp_column.insert(timestampToDateTime64(sample.timestamp(), timestamp_scale));
                value_column.insert(sample.value());
            }
        }

        BlocksToInsert res;

        /// A block to the tags table should be inserted first.
        /// (Because any INSERT can fail and we don't want to have rows in the data table with no corresponding 'id' written to the 'tags' table.)
        res.blocks.emplace_back(TargetTableKind::kTags, std::move(tags_block));
        res.blocks.emplace_back(TargetTableKind::kData, std::move(data_block));

        return res;
    }

    std::string_view metricTypeToString(prometheus::MetricMetadata::MetricType metric_type)
    {
        using namespace std::literals;
        switch (metric_type)
        {
            case prometheus::MetricMetadata::UNKNOWN: return "unknown"sv;
            case prometheus::MetricMetadata::COUNTER: return "counter"sv;
            case prometheus::MetricMetadata::GAUGE: return "gauge"sv;
            case prometheus::MetricMetadata::HISTOGRAM: return "histogram"sv;
            case prometheus::MetricMetadata::GAUGEHISTOGRAM: return "gaugehistogram"sv;
            case prometheus::MetricMetadata::SUMMARY: return "summary"sv;
            case prometheus::MetricMetadata::INFO: return "info"sv;
            case prometheus::MetricMetadata::STATESET: return "stateset"sv;
            default: break;
        }
        return "";
    }

    /// Converts metrics metadata in the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
    {
        size_t num_rows = metrics_metadata.size();

        if (!num_rows)
            return {}; /// Nothing to insert into target tables.

        /// We're going to prepare one blocks for the metadata table.
        Block block;

        auto add_column_to_block = [&](const String & column_name_, const DataTypePtr & type_) -> IColumn &
        {
            auto column = type_->createColumn();
            column->reserve(num_rows);
            auto * ptr = column.get();
            block.insert(ColumnWithTypeAndName{std::move(column), type_, column_name_});
            return *ptr;
        };

        /// Create columns.
        auto string_type = std::make_shared<DataTypeString>();

        auto & metric_family_name_column = typeid_cast<ColumnString &>(add_column_to_block(TimeSeriesColumnNames::kMetricFamilyName, string_type));
        auto & type_column               = typeid_cast<ColumnString &>(add_column_to_block(TimeSeriesColumnNames::kType,             string_type));
        auto & unit_column               = typeid_cast<ColumnString &>(add_column_to_block(TimeSeriesColumnNames::kUnit,             string_type));
        auto & help_column               = typeid_cast<ColumnString &>(add_column_to_block(TimeSeriesColumnNames::kHelp,             string_type));

        /// Fill columns.
        for (const auto & element : metrics_metadata)
        {
            const auto & metric_family_name = element.metric_family_name();
            const auto & type_str = metricTypeToString(element.type());
            const auto & help = element.help();
            const auto & unit = element.unit();
    
            metric_family_name_column.insertData(metric_family_name.data(), metric_family_name.length());
            type_column.insertData(type_str.data(), type_str.length());
            unit_column.insertData(unit.data(), unit.length());
            help_column.insertData(help.data(), help.length());
        }
    
        BlocksToInsert res;
        res.blocks.emplace_back(TargetTableKind::kMetrics, std::move(block));
        return res;
    }

    /// Inserts blocks to target tables.
    void insertToTargetTables(BlocksToInsert && blocks, StorageTimeSeries & time_series_storage, ContextPtr context, Poco::Logger * log)
    {
        auto time_series_storage_id = time_series_storage.getStorageID();

        for (auto & [table_kind, block] : blocks.blocks)
        {
            if (block)
            {
                const auto & target_table_id = time_series_storage.getTargetTableId(table_kind);

                LOG_INFO(log, "{}: Inserting {} rows to the {} table",
                         time_series_storage_id.getNameForLogs(), block.rows(), toString(table_kind));

                auto insert_query = std::make_shared<ASTInsertQuery>();
                insert_query->table_id = target_table_id;
    
                ContextMutablePtr insert_context = Context::createCopy(context);
                insert_context->setCurrentQueryId(context->getCurrentQueryId() + ":" + String{toString(table_kind)});
    
                InterpreterInsertQuery interpreter(insert_query, insert_context);
                BlockIO io = interpreter.execute();
                PushingPipelineExecutor executor(io.pipeline);
    
                executor.start();
                executor.push(std::move(block));
                executor.finish();
            }
        }
    }
}


PrometheusRemoteWriteProtocol::PrometheusRemoteWriteProtocol(StoragePtr time_series_storage_, const ContextPtr & context_)
    : time_series_storage(storagePtrToTimeSeries(time_series_storage_))
    , context(context_)
    , log(getLogger("PrometheusRemoteWriteProtocol"))
{
}

PrometheusRemoteWriteProtocol::~PrometheusRemoteWriteProtocol() = default;


void PrometheusRemoteWriteProtocol::writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} time series",
              time_series_storage_id.getNameForLogs(), time_series.size());

    auto time_series_settings = time_series_storage->getStorageSettingsPtr();
    auto id_calculator = time_series_storage->getIDCalculatorPtr();
    auto data_table_metadata = time_series_storage->getTargetTable(TargetTableKind::kData, context)->getInMemoryMetadataPtr();
    auto tags_table_metadata = time_series_storage->getTargetTable(TargetTableKind::kTags, context)->getInMemoryMetadataPtr();

    auto blocks = toBlocks(time_series, time_series_storage_id, *time_series_settings, *id_calculator, *data_table_metadata, *tags_table_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, context, log.get());

    LOG_TRACE(log, "{}: {} time series written",
              time_series_storage_id.getNameForLogs(), time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    auto time_series_storage_id = time_series_storage->getStorageID();

    LOG_TRACE(log, "{}: Writing {} metrics metadata",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());

    auto blocks = toBlocks(metrics_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, context, log.get());

    LOG_TRACE(log, "{}: {} metrics metadata written",
              time_series_storage_id.getNameForLogs(), metrics_metadata.size());
}

}

#endif
