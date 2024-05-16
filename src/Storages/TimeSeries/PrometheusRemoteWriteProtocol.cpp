#include <Storages/TimeSeries/PrometheusRemoteWriteProtocol.h>


namespace DB
{

namespace
{
    struct BlocksToInsert
    {
        std::vector<std::pair<TargetTableKind, Block>> blocks;
    };

    void insertToTimeSeriesTable(BlocksToInsert && blocks, StorageTimeSeries & destination_storage, ContextPtr context, Poco::Logger * log)
    {
        for (auto & [table_kind, block] : blocks.blocks)
        {
            if (block)
            {
                const auto & table_id = destination_storage.getTargetTableId(table_kind);
                LOG_INFO(log, "Inserting {} rows to table {}", block.rows(), table_id);
                auto insert_query = std::make_shared<ASTInsertQuery>();
                insert_query->table_id = table_id;
    
                ContextMutablePtr insert_context = Context::createCopy(context);
                insert_context->setCurrentQueryId(context->getCurrentQueryId() + ":" + toString(table_kind));
    
                InterpreterInsertQuery interpreter(insert_query, insert_context);
                BlockIO io = interpreter.execute();
                PushingPipelineExecutor executor(io.pipeline);
    
                executor.start();
                executor.push(std::move(block));
                executor.finish();
            }
        }
    }

    Block createBlock(const ColumnsDescription & columns_description)
    {
        Block block;
        for (const auto & column_description : columns_description)
        {
            block.insert(ColumnWithTypeAndName{column_description.type->createColumn(), column_description.type, column_description.name});
        }
        return block;
    }

    void reserveRowsInBlock(Block & block, size_t max_rows)
    {
        for (auto & column : block)
            column->assumeMutableRef().reserve(max_rows);
    }

    template <typename ColumnType>
    void assignColumnVar(ColumnType * & dest_column_ptr, const ColumnPtr & src_column)
    {
        dest_column_ptr = typeid_cast<ColumnType>(&src_column->assumeMutableRef());
    };

    void checkMetricNameAndTags(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels_including_metric_name)
    {
        for (size_t i = 0; i != labels_including_metric_name.size(); ++i)
        {
            const auto & label = labels_including_metric_name[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();

            if (label_name.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label name should not be empty");
            if (label_value.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label {} has an empty value", label_value);

            if (i)
            {
                const auto & previous_name = tags_including_metric_name[i - 1].name();
                if (label_name <= previous_name)
                {
                    if (label_name == previous_name)
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Found duplicate label {}", label_name);
                    else
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label names are not sorted in lexicographical order ({} > {})",
                                        previous_name, label_name);
                }
            }
        }
    }

    UInt128 calculateIdFromMetricNameAndTags(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels_including_metric_name)
    {
        SipHash sip_hash;
        for (size_t i = 0; i != labels_including_metric_name.size(); ++i)
        {
            const auto & label = labels_including_metric_name[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            sip_hash.update(label_name.data(), label_name.length());
            sip_hash.update(label_value.data(), label_value.length());
        }
        return sip_hash.get128();
    }

    size_t findMetricNameInTags(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels_including_metric_name)
    {
        for (size_t i = 0; i != labels_including_metric_name.size(); ++i)
        {
            const auto & label_name = label.name();
            if (label_name == "__name__")
                return i;
        }
        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label __name__ not found");
    }

    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
    {
        BlocksToInsert res;

        size_t tags_num_new_rows = time_series.size();

        size_t data_num_new_rows = 0;
        for (const auto & element : time_series)
            data_num_new_rows += element.samples_size();

        if (!data_num_new_rows)
            return res;

        Block & tags_block = res.blocks.emplace_back(TargetTableKind::kTags, createBlock(StorageTimeSeries::getColumnsDescription(TargetTableKind::kTags))).second;
        Block & data_block = res.blocks.emplace_back(TargetTableKind::kData, createBlock(StorageTimeSeries::getColumnsDescription(TargetTableKind::kData))).second;

        reserveRowsInBlock(data_block, data_num_new_rows);
        reserveRowsInBlock(tags_block, tags_num_new_rows);

        ColumnUInt128 * data_column_id = nullptr;
        ColumnDecimal<DateTime64> * data_column_timestamp = nullptr;
        ColumnFloat64 * data_column_value = nullptr;

        ColumnUInt128 * tags_column_id = nullptr;
        ColumnString * tags_column_metric_name = nullptr;
        ColumnMap * tags_column_tags = nullptr;
        ColumnString * tags_column_tags_names = nullptr;
        ColumnString * tags_column_tags_values = nullptr;
        ColumnArray::Offsets * tags_column_tags_offsets = nullptr;

        assignColumnVar(data_column_id,        data_block.getByName("id").column);
        assignColumnVar(data_column_timestamp, data_block.getByName("timestamp").column);
        assignColumnVar(data_column_value,     data_block.getByName("value").column);

        assignColumnVar(tags_column_id,          tags_block.getByName("id").column);
        assignColumnVar(tags_column_metric_name, tags_block.getByName("metric_name").column);
        assignColumnVar(tags_column_tags,        tags_block.getByName("tags").column);

        assignColumnVar(tags_column_tags_names, tags_column_tags->getNestedData().getColumnPtr(0));
        assignColumnVar(tags_column_tags_values, tags_column_tags->getNestedData().getColumnPtr(1));
        tags_column_tags_offsets = &tags_column_tags->getNestedColumn().getOffsets();

        for (const auto & element : time_series)
        {
            const auto & labels = element.labels();
            checkMetricNameAndTags(labels);
            UInt128 id = calculateIdFromMetricNameAndTags(labels);

            size_t metric_name_pos = findMetricNameInTags(labels);
            const auto & metric_name = labels[metric_name_pos].value();
            tags_column_metric_name->insertData(metric_name.data(), metric_name.length());

            for (int i = 0; i != labels.size(); ++i)
            {
                const auto & label = labels[i];
                const auto & label_name = label.name();
                const auto & label_value = label.value();

                if (i != metric_name_pos)
                {
                    tags_column_tags_names->insertData(label_name.data(), label_name.length());
                    tags_column_tags_values->insertData(label_value.data(), label_value.length());
                }
            }

            tags_column_tags_offsets->push_back(tags_column_tags_names->size());
            tags_column_id->insertValue(id);

            for (const auto & sample : element.samples())
            {
                data_column_id->insertValue(labels_hash);
                data_column_timestamp->insertValue(sample.timestamp());
                data_column_value->insertValue(sample.value());
            }
        }

        return res;
    }

    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
    {
        BlocksToInsert res;
    
        size_t num_new_rows = metrics_metadata.size();

        if (!num_new_rows)
            return res;

        Block & block = res.blocks.emplace_back(TargetTableKind::kMetrics, createBlock(StorageTimeSeries::getColumnsDescription(TargetTableKind::kMetrics))).second;
        reserveRowsInBlock(block, num_new_rows);

        ColumnString * column_metric_family_name = nullptr;
        ColumnString * column_type = nullptr;
        ColumnString * column_unit = nullptr;
        ColumnString * column_help = nullptr;
    
        assignColumnVar(column_metric_family_name, block.getByName("metric_family_name").column);
        assignColumnVar(column_type,               block.getByName("type").column);
        assignColumnVar(column_unit,               block.getByName("unit").column);
        assignColumnVar(column_help,               block.getByName("help").column);
    
        for (const auto & element : metrics_metadata)
        {
            const auto & metric_family_name = element.metric_family_name();
            const auto & type_str = metricTypeToString(element.type());
            const auto & help = element.help();
            const auto & unit = element.unit();
    
            column_metric_family_name->insertData(metric_family_name.data(), metric_family_name.length());
            column_type->insertData(type_str.data(), type_str.length());
            column_help->insertData(help.data(), help.length());
            column_unit->insertData(unit.data(), unit.length());
        }
    
        return res;
    }
}


PrometheusRemoteWriteProtocol::PrometheusRemoteWriteProtocol(StoragePtr storage_, const ContextPtr & context_)
    : storage(typeid_cast<std::shared_ptr<StorageTimeSeries>>(storage_))
    , context(context_)
    , log(getLogger("PrometheusRemoteWriteProtocol"))
{
    if (!storage)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENT,
            "PrometheusRemoteWriteProtocol can be used with a TimeSeries table engine only, {} has engine {}",
            storage_->getStorageID().getNameForLogs(),
            storage_->getName());
    }
}

PrometheusRemoteWriteProtocol::~PrometheusRemoteWriteProtocol() = default;

void PrometheusRemoteWriteProtocol::writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series)
{
    LOG_TRACE(log, "Writing {} time series", time_series.size());
    insertToTimeSeriesTable(toBlocks(time_series), *storage, context, log.get());
    LOG_TRACE(log, "{} time series have been written", time_series.size());
}

void PrometheusRemoteWriteProtocol::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata)
{
    LOG_TRACE(log, "Writing {} metrics metadata", time_series.size());
    insertToTimeSeriesTable(toBlocks(metrics_metadata), *storage, context, log.get());
    LOG_TRACE(log, "{} metrics metadata has been written", time_series.size());
}

}
