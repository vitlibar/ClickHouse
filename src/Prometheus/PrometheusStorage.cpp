#include <Prometheus/PrometheusStorage.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/getOrCreateSystemFilledTable.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_PROMETHEUS_LABELS;
}


namespace
{
    using TableKind = PrometheusStorage::TableKind;

    const String DEFAULT_DATABASE_NAME = "prometheus";

    String getDefaultTableName(TableKind table_kind)
    {
        switch (table_kind)
        {
            case TableKind::TIME_SERIES:    return "time_series";
            case TableKind::LABELS:         return "labels";
            case TableKind::LABELS_BY_NAME: return "labels_by_name";
            case TableKind::MAX: break;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table kind: {}", table_kind);
    }

    ColumnsDescription getColumnsDescription(TableKind table_kind)
    {
        switch (table_kind)
        {
            case TableKind::TIME_SERIES:
            {
                return ColumnsDescription{
                    { "labels_hash", std::make_shared<DataTypeUInt128>()     },
                    { "timestamp",   std::make_shared<DataTypeDateTime64>(3) },
                    { "value",       std::make_shared<DataTypeFloat64>()     }
                };
            }

            case TableKind::LABELS:
            {
                return ColumnsDescription{
                    { "labels",      std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()) },
                    { "labels_hash", std::make_shared<DataTypeUInt128>()                                                                   }
                };
            }

            case TableKind::LABELS_BY_NAME:
            {
                return ColumnsDescription{
                    { "label_name",         std::make_shared<DataTypeString>()  },
                    { "label_value",        std::make_shared<DataTypeString>()  },
                    { "labels_hash",        std::make_shared<DataTypeUInt128>() },
                    { "num_labels_in_hash", std::make_shared<DataTypeUInt64>()  }
                };
            }

            case TableKind::MAX: break;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table kind: {}", table_kind);
    }

    String getDefaultTableEngine(TableKind table_kind)
    {
        switch (table_kind)
        {
            case TableKind::TIME_SERIES:
                return "ENGINE=MergeTree ORDER BY (labels_hash, timestamp)";

            case TableKind::LABELS:
                return "ENGINE=ReplacingMergeTree ORDER BY labels_hash";

            case TableKind::LABELS_BY_NAME:
                return "ENGINE=ReplacingMergeTree ORDER BY (label_name, label_value, labels_hash)";

            case TableKind::MAX: break;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table kind: {}", table_kind);
    }

    String getConfigSectionForTable(const String & prometheus_storage_id, TableKind table_kind, const Poco::Util::AbstractConfiguration & config)
    {
        String config_section = "prometheus.storages." + prometheus_storage_id;
        if (!config.has(config_section))
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Section {} was not found in the config", config_section);

        return config_section + "." + getDefaultTableName(table_kind);
    }

    void getLabelsFromProtobuf(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & pb_labels, std::vector<std::pair<std::string_view, std::string_view>> & out_labels)
    {
        out_labels.resize(pb_labels.size());
        for (int i = 0; i != pb_labels.size(); ++i)
        {
            const auto & pb_label = pb_labels[i];
            out_labels[i].first = pb_label.name();
            out_labels[i].second = pb_label.value();
        }
    }

    void checkLabels(const std::vector<std::pair<std::string_view, std::string_view>> & labels)
    {
        bool found_name_label = false;

        for (const auto & [label_name, label_value] : labels)
        {
            if (label_name.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label name should not be empty");
            if (label_value.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label {} has an empty value", label_value);
            if (label_name == "__name__")
                found_name_label = true;
        }

        if (!found_name_label)
            throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label __name__ not found");

        if (labels.size() >= 2)
        {
            for (size_t i = 0; i != labels.size() - 1; ++i)
            {
                if (labels[i].first >= labels[i + 1].first)
                {
                    if (labels[i].first == labels[i + 1].first)
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Found duplicate label {}", labels[i].first);
                    else
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label names are not sorted in lexicographical order ({} > {})",
                                        labels[i].first, labels[i + 1].first);
                }
            }
        }
    }

    UInt128 calculateLabelsHash(const std::vector<std::pair<std::string_view, std::string_view>> & labels)
    {
        SipHash sip_hash;
        for (const auto & [label_name, label_value] : labels)
        {
            sip_hash.update(label_name.data(), label_name.length());
            sip_hash.update(label_value.data(), label_value.length());
        }
        return sip_hash.get128();
    }
}


PrometheusStorage::PrometheusStorage(const String & prometheus_storage_id_)
    : prometheus_storage_id(prometheus_storage_id_)
    , log(getLogger("PrometheusStorage(" + prometheus_storage_id + ")"))
    , table_ids{StorageID::createEmpty(), StorageID::createEmpty(), StorageID::createEmpty()}
{
    for (auto table_kind : collections::range(TableKind::MAX))
        columns_descriptions[static_cast<size_t>(table_kind)] = getColumnsDescription(table_kind);
}

PrometheusStorage::~PrometheusStorage() = default;


void PrometheusStorage::reloadConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    try
    {
        loadConfig(config, nullptr);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to update configuration");
    }
}


void PrometheusStorage::ensureConfigLoaded(const Poco::Util::AbstractConfiguration & config)
{
    std::unique_lock lock{mutex};
    if (!TSA_SUPPRESS_WARNING_FOR_READ(tables_config_loaded))
        loadConfig(config, &lock);
}


void PrometheusStorage::loadConfig(const Poco::Util::AbstractConfiguration & config, std::unique_lock<std::mutex> * mutex_is_locked)
{
    TableConfig new_tables_config[static_cast<size_t>(TableKind::MAX)];

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        String config_section = getConfigSectionForTable(prometheus_storage_id, table_kind, config);

        TableConfig table_config;
        table_config.database_name = config.getString(config_section + ".database", DEFAULT_DATABASE_NAME);
        table_config.table_name = config.getString(config_section + ".table", getDefaultTableName(table_kind));
        table_config.engine = config.getString(config_section + ".engine", getDefaultTableEngine(table_kind));

        /// Validate engine definition syntax to prevent some configuration errors.
        ParserStorageWithComment storage_parser{ParserStorage::TABLE_ENGINE};

        parseQuery(storage_parser, table_config.engine.data(), table_config.engine.data() + table_config.engine.size(),
                   "Storage to create table " + table_config.table_name + "(prometheus storage " + prometheus_storage_id + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        new_tables_config[static_cast<size_t>(table_kind)] = table_config;
    }

    {
        std::unique_lock lock{mutex, std::defer_lock};
        if (!mutex_is_locked)
            lock.lock();

        bool config_changed = false;
        for (auto table_kind : collections::range(TableKind::MAX))
        {
            const auto & new_table_config = new_tables_config[static_cast<size_t>(table_kind)];
            auto & table_config = tables_config[static_cast<size_t>(table_kind)];
            if (table_config != new_table_config)
            {
                table_config = new_table_config;
                config_changed = true;
                if (tables_config_loaded)
                    LOG_INFO(log, "Configuration changed");
            }
        }

        if (config_changed)
        {
            /// We need to check or recreate our table once again because the config has changed.
            create_queries_prepared = false;
            tables_created = false;
            for (auto table_kind : collections::range(TableKind::MAX))
            {
                create_queries[static_cast<size_t>(table_kind)] = nullptr;
                table_ids[static_cast<size_t>(table_kind)] = StorageID::createEmpty();
            }
        }

        tables_config_loaded = true;
    }
}


void PrometheusStorage::prepareCreateQueries(ContextPtr global_context)
{
    std::lock_guard lock{mutex};
    if (create_queries_prepared)
        return;

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        const auto & table_config = tables_config[static_cast<size_t>(table_kind)];
        const auto & columns_description = columns_descriptions[static_cast<size_t>(table_kind)];

        auto create = std::make_shared<ASTCreateQuery>();

        create->setDatabase(table_config.database_name);
        create->setTable(table_config.table_name);

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(columns_description));
        create->set(create->columns_list, new_columns_list);

        ParserStorageWithComment storage_parser{ParserStorage::TABLE_ENGINE};

        ASTPtr storage_with_comment_ast = parseQuery(
            storage_parser, table_config.engine.data(), table_config.engine.data() + table_config.engine.size(),
            "Storage to create table " + table_config.table_name + "(prometheus storage " + prometheus_storage_id + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        const auto & storage_with_comment = storage_with_comment_ast->as<const ASTStorageWithComment &>();
        create->set(create->storage, storage_with_comment.storage);
        create->set(create->comment, storage_with_comment.comment);

        /// Write additional (default) settings for MergeTree engine to make it make it possible to compare ASTs
        /// and recreate tables on settings changes.
        const auto & engine = create->storage->engine->as<ASTFunction &>();
        if (endsWith(engine.name, "MergeTree"))
        {
            auto storage_settings = std::make_unique<MergeTreeSettings>(global_context->getMergeTreeSettings());
            storage_settings->loadFromQuery(*create->storage, global_context, false);
        }

        create_queries[static_cast<size_t>(table_kind)] = create;
    }

    create_queries_prepared = true;
}


void PrometheusStorage::ensureTablesCreated(ContextPtr global_context)
{
    std::lock_guard lock{mutex};
    if (tables_created)
        return;

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        const auto & create_query = create_queries[static_cast<size_t>(table_kind)];
        bool check_create_query_if_exists = !tables_created;
        auto [storage, created] = getOrCreateSystemFilledTable(global_context, create_query, check_create_query_if_exists);
        auto table_id = storage->getStorageID();
        table_ids[static_cast<size_t>(table_kind)] = table_id;
        if (!created)
            LOG_DEBUG(log, "Will use existing table {}", table_id.getNameForLogs());
    }

    tables_created = true;
}


void PrometheusStorage::prepareTables(ContextPtr global_context)
{
    chassert(global_context == global_context->getGlobalContext());
    ensureConfigLoaded(global_context->getConfigRef());
    prepareCreateQueries(global_context);
    ensureTablesCreated(global_context);
}


struct PrometheusStorage::BlocksToInsert
{
    Block blocks[static_cast<size_t>(TableKind::MAX)];
};


void PrometheusStorage::addTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series, ContextPtr context)
{
    LOG_TRACE(log, "Adding time series");

    prepareTables(context->getGlobalContext());
    insertToTables(toBlocks(time_series), context);

    LOG_TRACE(log, "Added time series");
}


PrometheusStorage::BlocksToInsert PrometheusStorage::toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series) const
{
    BlocksToInsert res;

    size_t num_rows_for_table_kind[static_cast<size_t>(TableKind::MAX)] = {0};
    size_t & num_rows_for_time_series = num_rows_for_table_kind[static_cast<size_t>(TableKind::TIME_SERIES)];
    size_t & num_rows_for_labels = num_rows_for_table_kind[static_cast<size_t>(TableKind::LABELS)];
    size_t & num_rows_for_labels_by_name = num_rows_for_table_kind[static_cast<size_t>(TableKind::LABELS_BY_NAME)];

    num_rows_for_labels = time_series.size();
    for (const auto & element : time_series)
    {
        num_rows_for_time_series += element.samples_size();
        num_rows_for_labels_by_name += element.samples_size();
    }

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        size_t num_rows = num_rows_for_table_kind[static_cast<size_t>(table_kind)];
        if (num_rows)
        {
            const ColumnsDescription & columns_description = columns_descriptions[static_cast<size_t>(table_kind)];
            Block & block = res.blocks[static_cast<size_t>(table_kind)];
            for (const auto & column_description : columns_description)
            {
                auto column = column_description.type->createColumn();
                column->reserve(num_rows);
                block.insert(ColumnWithTypeAndName{std::move(column), column_description.type, column_description.name});
            }
        }
    }

    ColumnUInt128 * time_series_col_labels_hash = nullptr;
    ColumnDecimal<DateTime64> * time_series_col_timestamp = nullptr;
    ColumnFloat64 * time_series_col_value = nullptr;

    ColumnMap * labels_col_labels = nullptr;
    ColumnString * labels_col_labels_names = nullptr;
    ColumnString * labels_col_labels_values = nullptr;
    ColumnArray::Offsets * labels_col_labels_offsets = nullptr;
    ColumnUInt128 * labels_col_labels_hash = nullptr;

    ColumnString * labels_by_name_col_label_name = nullptr;
    ColumnString * labels_by_name_col_label_value = nullptr;
    ColumnUInt128 * labels_by_name_col_labels_hash = nullptr;
    ColumnUInt64 * labels_by_name_col_num_labels_in_hash = nullptr;

    auto assign_column_var = [](auto * & dest_column_ptr, const ColumnPtr & src_column)
    {
        dest_column_ptr = typeid_cast<std::remove_cvref_t<decltype(dest_column_ptr)>>(&src_column->assumeMutableRef());
    };

    auto assign_column_var_from_result = [&](auto * & dest_column_ptr, TableKind table_kind, size_t column_pos)
    {
        assign_column_var(dest_column_ptr, res.blocks[static_cast<size_t>(table_kind)].getByPosition(column_pos).column);
    };

    if (num_rows_for_time_series)
    {
        assign_column_var_from_result(time_series_col_labels_hash, TableKind::TIME_SERIES, 0);
        assign_column_var_from_result(time_series_col_timestamp,   TableKind::TIME_SERIES, 1);
        assign_column_var_from_result(time_series_col_value,       TableKind::TIME_SERIES, 2);
    }

    if (num_rows_for_labels)
    {
        assign_column_var_from_result(labels_col_labels,      TableKind::LABELS, 0);
        assign_column_var_from_result(labels_col_labels_hash, TableKind::LABELS, 1);

        assign_column_var(labels_col_labels_names, labels_col_labels->getNestedData().getColumnPtr(0));
        assign_column_var(labels_col_labels_values, labels_col_labels->getNestedData().getColumnPtr(1));
        labels_col_labels_offsets = &labels_col_labels->getNestedColumn().getOffsets();

        labels_col_labels_names->reserve(num_rows_for_labels_by_name);
        labels_col_labels_values->reserve(num_rows_for_labels_by_name);
    }

    if (num_rows_for_labels_by_name)
    {
        assign_column_var_from_result(labels_by_name_col_label_name,         TableKind::LABELS_BY_NAME, 0);
        assign_column_var_from_result(labels_by_name_col_label_value,        TableKind::LABELS_BY_NAME, 1);
        assign_column_var_from_result(labels_by_name_col_labels_hash,        TableKind::LABELS_BY_NAME, 2);
        assign_column_var_from_result(labels_by_name_col_num_labels_in_hash, TableKind::LABELS_BY_NAME, 3);
    }

    std::vector<std::pair<std::string_view, std::string_view>> labels;

    for (const auto & element : time_series)
    {
        getLabelsFromProtobuf(element.labels(), labels);
        checkLabels(labels);
        auto labels_hash = calculateLabelsHash(labels);

        for (const auto & [label_name, label_value] : labels)
        {
            labels_col_labels_names->insertData(label_name.data(), label_name.length());
            labels_col_labels_values->insertData(label_value.data(), label_value.length());

            labels_by_name_col_label_name->insertData(label_name.data(), label_name.length());
            labels_by_name_col_label_value->insertData(label_value.data(), label_value.length());
            labels_by_name_col_labels_hash->insertValue(labels_hash);
            labels_by_name_col_num_labels_in_hash->insertValue(labels.size());
        }

        labels_col_labels_offsets->push_back(labels_col_labels_names->size());
        labels_col_labels_hash->insertValue(labels_hash);

        for (const auto & sample : element.samples())
        {
            time_series_col_labels_hash->insertValue(labels_hash);
            time_series_col_timestamp->insertValue(sample.timestamp());
            time_series_col_value->insertValue(sample.value());
        }
    }

    return res;
}


void PrometheusStorage::insertToTables(BlocksToInsert && blocks, ContextPtr context)
{
    StorageID table_ids_to_insert[static_cast<size_t>(TableKind::MAX)] = {StorageID::createEmpty(), StorageID::createEmpty(), StorageID::createEmpty()};
    {
        std::lock_guard lock{mutex};
        for (auto table_kind : collections::range(TableKind::MAX))
            table_ids_to_insert[static_cast<size_t>(table_kind)] = table_ids[static_cast<size_t>(table_kind)];
    }

    /// We use this specific order of insertion because we want our tables to be consistent even if some of the following insertion fail.
    /// That's why we insert to the "time_series" table at the end.
    TableKind insert_order[] = {TableKind::LABELS, TableKind::LABELS_BY_NAME, TableKind::TIME_SERIES};
    static_assert(std::size(insert_order) == static_cast<size_t>(TableKind::MAX));

    for (auto table_kind : insert_order)
    {
        const auto & table_id = table_ids_to_insert[static_cast<size_t>(table_kind)];
        auto & block = blocks.blocks[static_cast<size_t>(table_kind)];
        if (block)
        {
            LOG_INFO(log, "Inserting {} rows to table {}", block.rows(), table_id);
            auto insert_query = std::make_shared<ASTInsertQuery>();
            insert_query->table_id = table_id;

            ContextMutablePtr insert_context = Context::createCopy(context);
            insert_context->setCurrentQueryId("");
            CurrentThread::QueryScope query_scope{insert_context};

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
