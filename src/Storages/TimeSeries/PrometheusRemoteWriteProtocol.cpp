#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Common/SipHash.h>
#include <Core/Field.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int DUPLICATE_COLUMN;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TIME_SERIES_TAGS;
    extern const int WRONG_TABLE_ENGINE;
}


namespace
{
    /// Calculates an ID for a specified set of labels using a specified algorithm.
    Field calculateId(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels, TimeSeriesIdAlgorithm algorithm)
    {
        SipHash sip_hash;
        for (const auto & label : labels)
        {
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            sip_hash.update(label_name.data(), label_name.length());
            sip_hash.update(label_value.data(), label_value.length());
        }
        switch (algorithm)
        {
            case TimeSeriesIdAlgorithm::SipHash64: return sip_hash.get64();
            case TimeSeriesIdAlgorithm::SipHash128: return sip_hash.get128();
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected id algorithm {}", algorithm);
    }

    /// Returns the type which calculateId() returns when using a specified algorithm.
    TypeIndex getIdAlgorithmResultType(TimeSeriesIdAlgorithm algorithm)
    {
        switch (algorithm)
        {
            case TimeSeriesIdAlgorithm::SipHash64: return TypeIndex::UInt64;
            case TimeSeriesIdAlgorithm::SipHash128: return TypeIndex::UInt128;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected id algorithm {}", algorithm);
    }

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

            if (label_name == "__name__")
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

    /// The `tags_to_columns` setting in the storage settings could specify multiple tags to put into the same column, which is illegal.
    /// Checks if it's the case and returns such tags and column name.
    bool findTwoTagsToSameColumn(const TimeSeriesSettings & time_series_storage_settings, String & out_tag_name1, String & out_tag_name2, String & out_column_name)
    {
        std::unordered_map<std::string_view, std::string_view> column_name_to_tag_name;
        const Map & tags_to_columns = time_series_storage_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            auto it = column_name_to_tag_name.find(column_name);
            if (it != column_name_to_tag_name.end())
            {
                out_column_name = column_name;
                out_tag_name1 = it->second;
                out_tag_name2 = tag_name;
                return true;
            }
            column_name_to_tag_name[column_name] = tag_name;
        }
        return false;
    }

    struct BlocksToInsert
    {
        std::vector<std::pair<TargetTableKind, Block>> blocks;
    };

    /// Converts time series in the protobuf format to prepared blocks for inserting into target tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series,
                            const StorageID & time_series_storage_id,
                            const TimeSeriesSettings & time_series_storage_settings,
                            const StorageID & data_table_id,
                            const StorageInMemoryMetadata & data_table_metadata,
                            const StorageID & tags_table_id,
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
        auto id_type = data_table_description.getColumn(insertable_columns, "id").type;
        auto timestamp_type = data_table_description.getColumn(insertable_columns, "timestamp").type;
        auto value_type = data_table_description.getColumn(insertable_columns, "value").type;
        auto id_type_in_tags_table = tags_table_description.getColumn(insertable_columns, "id").type;

        /// Check data types.
        auto id_algorithm = time_series_storage_settings.id_algorithm;
        auto expected_id_type = getIdAlgorithmResultType(id_algorithm);
        if (id_type->getTypeId() != expected_id_type)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The 'id' column in data table {} has wrong data type {}, {} is expected according to the settings",
                            time_series_storage_id.getNameForLogs(), data_table_id.getNameForLogs(), id_type->getName(), expected_id_type);
        }

        if (id_type_in_tags_table->getTypeId() != expected_id_type)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The 'id' column in tags table {} has wrong data type {}, {} is expected according to the settings",
                            time_series_storage_id.getNameForLogs(), tags_table_id.getNameForLogs(), id_type_in_tags_table->getName(), expected_id_type);
        }

        if (timestamp_type->getTypeId() != TypeIndex::DateTime64)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The 'timestamp' column in data table {} has wrong data type {}, DateTime64(s) is expected where s >= 3",
                            time_series_storage_id.getNameForLogs(), data_table_id.getNameForLogs(), timestamp_type->getName());
        }

        auto timestamp_scale = typeid_cast<const DataTypeDateTime64 &>(*timestamp_type).getScale();
        if (timestamp_scale < 3)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The 'timestamp' column in data table {} has wrong data type {}, DateTime64(s) is expected where s >= 3",
                            time_series_storage_id.getNameForLogs(), data_table_id.getNameForLogs(), timestamp_type->getName());
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
        auto & id_column_in_data_table = add_column_to_data_block("id", id_type);
        auto & timestamp_column = add_column_to_data_block("timestamp", timestamp_type);
        auto & value_column = add_column_to_data_block("value", value_type);

        auto & id_column_in_tags_table = add_column_to_tags_block("id", id_type);

        std::vector<IColumn *> tag_columns;
        std::unordered_map<String, IColumn *> tag_columns_by_tag_name;

        auto add_tag_column = [&](const String & tag_name, const String & column_name)
        {
            auto data_type = tags_table_description.getColumn(insertable_columns, column_name).type;
            if ((data_type->getTypeId() != TypeIndex::String) && (data_type->getTypeId() != TypeIndex::LowCardinality))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "{}: The '{}' column in tags table {} has wrong data type {}, String or LowCardinality(String) is expected",
                                time_series_storage_id.getNameForLogs(), column_name, tags_table_id.getNameForLogs(), data_type->getName());
            }
            auto & column = add_column_to_tags_block(column_name, data_type);
            tag_columns.emplace_back(&column);
            tag_columns_by_tag_name[tag_name] = &column;
        };

        add_tag_column("__name__", "metric_name");

        const Map & tags_to_columns = time_series_storage_settings.tags_to_columns;
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
            const auto & tag_name = tuple.at(0).safeGet<String>();
            const auto & column_name = tuple.at(1).safeGet<String>();
            add_tag_column(tag_name, column_name);
        }

        auto string_type = std::make_shared<DataTypeString>();
        auto other_tags_type = std::make_shared<DataTypeMap>(string_type, string_type);
        auto & other_tags_column = typeid_cast<ColumnMap &>(add_column_to_tags_block("tags", other_tags_type));
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
            auto id = calculateId(labels, id_algorithm);

            labels_written.clear();

            for (size_t j = 0; j != static_cast<size_t>(labels.size()); ++j)
            {
                const auto & label = labels[static_cast<int>(j)];
                const auto & label_name = label.name();
                const auto & label_value = label.value();

                auto it = tag_columns_by_tag_name.find(label_name);
                if (it != tag_columns_by_tag_name.end())
                {
                    auto * tag_column = it->second;
                    if (tag_column->size() != i)
                    {
                        String tag_name1, tag_name2, column_name;
                        findTwoTagsToSameColumn(time_series_storage_settings, tag_name1, tag_name2, column_name);
                        throw Exception(ErrorCodes::DUPLICATE_COLUMN, "{}: Can't insert multiple tags {} and {} to the same column {}",
                                        time_series_storage_id.getNameForLogs(), tag_name1, tag_name2, column_name);
                    }
                    tag_column->insertData(label_value.data(), label_value.length());
                    if (labels_written.size() < j + 1)
                        labels_written.resize(j + 1, false);
                    labels_written[j] = true;
                }
            }

            for (auto * tag_column : tag_columns)
            {
                if (tag_column->size() < i + 1)
                    tag_column->insertDefault();
            }

            for (int j = 0; j != labels.size(); ++j)
            {
                if (!labels_written[i])
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

        auto & metric_family_name_column = typeid_cast<ColumnString &>(add_column_to_block("metric_family_name", string_type));
        auto & type_column = typeid_cast<ColumnString &>(add_column_to_block("type", string_type));
        auto & unit_column = typeid_cast<ColumnString &>(add_column_to_block("unit", string_type));
        auto & help_column = typeid_cast<ColumnString &>(add_column_to_block("help", string_type));

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
        for (auto & [table_kind, block] : blocks.blocks)
        {
            if (block)
            {
                const auto & table_id = time_series_storage.getTargetTableId(table_kind);
                LOG_INFO(log, "Inserting {} rows to table {}", block.rows(), table_id);
                auto insert_query = std::make_shared<ASTInsertQuery>();
                insert_query->table_id = table_id;
    
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
    : context(context_)
    , log(getLogger("PrometheusRemoteWriteProtocol"))
{
    time_series_storage = typeid_cast<std::shared_ptr<StorageTimeSeries>>(time_series_storage_);
    if (!time_series_storage)
    {
        throw Exception(
            ErrorCodes::WRONG_TABLE_ENGINE,
            "PrometheusRemoteWriteProtocol can be used with a TimeSeries table engine only, {} has engine {}",
            time_series_storage_->getStorageID().getNameForLogs(),
            time_series_storage_->getName());
    }
}

PrometheusRemoteWriteProtocol::~PrometheusRemoteWriteProtocol() = default;


void PrometheusRemoteWriteProtocol::writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
{
    LOG_TRACE(log, "Writing {} time series", time_series.size());

    auto time_series_storage_id = time_series_storage->getStorageID();
    auto time_series_storage_settings = time_series_storage->getStorageSettingsPtr();
    auto data_table_id = time_series_storage->getTargetTableId(TargetTableKind::kData);
    auto data_table_metadata = time_series_storage->getTargetTable(TargetTableKind::kData, context)->getInMemoryMetadataPtr();
    auto tags_table_id = time_series_storage->getTargetTableId(TargetTableKind::kTags);
    auto tags_table_metadata = time_series_storage->getTargetTable(TargetTableKind::kTags, context)->getInMemoryMetadataPtr();

    auto blocks = toBlocks(time_series, time_series_storage_id, *time_series_storage_settings, data_table_id, *data_table_metadata, tags_table_id, *tags_table_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, context, log.get());

    LOG_TRACE(log, "{} time series have been written", time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    LOG_TRACE(log, "Writing {} metrics metadata", metrics_metadata.size());

    auto blocks = toBlocks(metrics_metadata);
    insertToTargetTables(std::move(blocks), *time_series_storage, context, log.get());

    LOG_TRACE(log, "{} metrics metadata has been written", metrics_metadata.size());
}

}
