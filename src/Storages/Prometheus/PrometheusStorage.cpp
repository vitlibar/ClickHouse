#include <Storages/Prometheus/PrometheusStorage.h>

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
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/getOrCreateSystemFilledTable.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int PROMETHEUS_STORAGE_NOT_FOUND;
    extern const int INVALID_PROMETHEUS_LABELS;
    extern const int BAD_REQUEST_PARAMETER;
    extern const int LOGICAL_ERROR;
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
            case TableKind::METRICS_METADATA: return "metrics_metadata";
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
                    { "value",       std::make_shared<DataTypeFloat64>()     },
                };
            }

            case TableKind::LABELS:
            {
                return ColumnsDescription{
                    { "metric_name", std::make_shared<DataTypeString>()                                                                    },
                    { "labels",      std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()) },
                    { "labels_hash", std::make_shared<DataTypeUInt128>()                                                                   },
                };
            }

            case TableKind::METRICS_METADATA:
            {
                return ColumnsDescription{
                    { "metric_family_name", std::make_shared<DataTypeString>() },
                    { "type",               std::make_shared<DataTypeString>() },
                    { "help",               std::make_shared<DataTypeString>() },
                    { "unit",               std::make_shared<DataTypeString>() },
                };
            }

            case TableKind::MAX: break;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table kind: {}", table_kind);
    }

    void createColumns(Block & block, const ColumnsDescription & columns_description, size_t reserve_num_rows)
    {
        for (const auto & column_description : columns_description)
        {
            auto column = column_description.type->createColumn();
            column->reserve(reserve_num_rows);
            block.insert(ColumnWithTypeAndName{std::move(column), column_description.type, column_description.name});
        }
    }

    String getDefaultTableEngine(TableKind table_kind)
    {
        switch (table_kind)
        {
            case TableKind::TIME_SERIES:
                return "ENGINE=MergeTree ORDER BY (labels_hash, timestamp)";

            case TableKind::LABELS:
                return "ENGINE=ReplacingMergeTree ORDER BY (metric_name, labels)";

            case TableKind::METRICS_METADATA:
                return "ENGINE=ReplacingMergeTree ORDER BY metric_family_name";

            case TableKind::MAX: break;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected table kind: {}", table_kind);
    }

    String getConfigSectionForTable(const String & prometheus_storage_id, TableKind table_kind, const Poco::Util::AbstractConfiguration & config)
    {
        String config_section = "prometheus.storages." + prometheus_storage_id;
        if (!config.has(config_section))
            throw Exception(ErrorCodes::PROMETHEUS_STORAGE_NOT_FOUND, "Prometheus storage {} not found in the config", config_section);

        return config_section + "." + getDefaultTableName(table_kind);
    }

    int checkLabels(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels)
    {
        int name_label_pos = -1;

        for (int i = 0; i != labels.size(); ++i)
        {
            const auto & label = labels[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            if (label_name.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label name should not be empty");
            if (label_value.empty())
                throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label {} has an empty value", label_value);
            if (label_name == "__name__")
                name_label_pos = i;
        }

        if (name_label_pos == -1)
            throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label __name__ not found");

        if (labels.size() >= 2)
        {
            for (int i = 1; i != labels.size(); ++i)
            {
                const auto & label = labels[i];
                const auto & previous_label = labels[i - 1];
                if (label.name() <= previous_label.name())
                {
                    if (label.name() == previous_label.name())
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Found duplicate label {}", label.name());
                    else
                        throw Exception(ErrorCodes::INVALID_PROMETHEUS_LABELS, "Label names are not sorted in lexicographical order ({} > {})",
                                        previous_label.name(), label.name());
                }
            }
        }

        return name_label_pos;
    }

    UInt128 calculateLabelsHash(const ::google::protobuf::RepeatedPtrField<::prometheus::Label> & labels)
    {
        SipHash sip_hash;
        for (int i = 0; i != labels.size(); ++i)
        {
            const auto & label = labels[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();
            sip_hash.update(label_name.data(), label_name.length());
            sip_hash.update(label_value.data(), label_value.length());
        }
        return sip_hash.get128();
    }

    void sortLabels(std::vector<std::pair<std::string_view, std::string_view>> & labels)
    {
        auto less_by_label_name = [](const std::pair<std::string_view, std::string_view> & left, const std::pair<std::string_view, std::string_view> & right)
        {
            return left.first < right.first;
        };

        std::sort(labels.begin(), labels.end(), less_by_label_name);
    }

    void sortTimeSeries(std::vector<std::pair<Int64, Float64>> & time_series)
    {
        auto less_by_timestamp = [](const std::pair<Int64, Float64> & left, const std::pair<Int64, Float64> & right)
        {
            return left.first < right.first;
        };

        std::sort(time_series.begin(), time_series.end(), less_by_timestamp);
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

    /// Makes an AST for condition `timestamp >= start_timestamp_ms`
    ASTPtr makeASTFilterForMinTimestamp(Int64 start_timestamp_ms)
    {
        return makeASTFunction("greaterOrEquals", std::make_shared<ASTIdentifier>("timestamp"),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{start_timestamp_ms}, 3}}));
    }

    /// Makes an AST for condition `timestamp <= end_timestamp_ms`
    ASTPtr makeASTFilterForMaxTimestamp(Int64 end_timestamp_ms)
    {
        return makeASTFunction("lessOrEquals", std::make_shared<ASTIdentifier>("timestamp"),
                               std::make_shared<ASTLiteral>(Field{DecimalField{DateTime64{end_timestamp_ms}, 3}}));
    }

    ASTPtr makeASTForLabelName(const String & label_name)
    {
        if (label_name == "__name__")
            return std::make_shared<ASTIdentifier>("metric_name");
        else
            return makeASTFunction("arrayElement", std::make_shared<ASTIdentifier>("labels"), std::make_shared<ASTLiteral>(label_name));
    }

    /// Makes an AST for the label matcher, for example `metric_name == 'value'` or `NOT match(labels['label_name'], 'regexp')`.
    ASTPtr makeASTFilterForLabelMatcher(const prometheus::LabelMatcher & label_matcher)
    {
        const auto & label_name = label_matcher.name();
        const auto & label_value = label_matcher.value();
        auto type = label_matcher.type();

        if (type == prometheus::LabelMatcher::EQ)
            return makeASTFunction("equals", makeASTForLabelName(label_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::NEQ)
            return makeASTFunction("notEquals", makeASTForLabelName(label_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::RE)
            return makeASTFunction("match", makeASTForLabelName(label_name), std::make_shared<ASTLiteral>(label_value));
        else if (type == prometheus::LabelMatcher::NRE)
            return makeASTFunction("not", makeASTFunction("match", makeASTForLabelName(label_name), std::make_shared<ASTLiteral>(label_value)));
        else
            throw Exception(ErrorCodes::BAD_REQUEST_PARAMETER, "Unexpected type of label matcher: {}", type);
    }

    ASTPtr makeASTFilterForReadingTimeSeries(Int64 start_timestamp_ms,
                                             Int64 end_timestamp_ms,
                                             const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher)
    {
        ASTs filters;

        if (start_timestamp_ms)
            filters.push_back(makeASTFilterForMinTimestamp(start_timestamp_ms));

        if (end_timestamp_ms)
            filters.push_back(makeASTFilterForMaxTimestamp(end_timestamp_ms));

        for (const auto & label_matcher_element : label_matcher)
            filters.push_back(makeASTFilterForLabelMatcher(label_matcher_element));

        if (filters.empty())
            return nullptr;
        
        return makeASTForLogicalAnd(std::move(filters));
    }

    /// Makes the following AST:
    /// SELECT any(metric_name), any(labels), groupArray((timestamp, value))
    /// FROM prometheus.time_series
    /// SEMI LEFT JOIN prometheus.labels ON prometheus.time_series.labels_hash = prometheus.labels.labels_hash
    /// WHERE <filter>
    /// GROUP BY labels_hash
    ASTPtr buildSelectQueryForReadingTimeSeriesImpl(const StorageID & time_series_table_id,
                                                    const StorageID & labels_table_id,
                                                    Int64 start_timestamp_ms,
                                                    Int64 end_timestamp_ms,
                                                    const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matcher)
    {
        auto select_query = std::make_shared<ASTSelectQuery>();

        /// SELECT any(metric_name), any(labels), groupArray((timestamp, value))
        {
            auto exp_list = std::make_shared<ASTExpressionList>();

            exp_list->children.push_back(
                makeASTFunction("any",
                                std::make_shared<ASTIdentifier>("metric_name")));

            exp_list->children.push_back(
                makeASTFunction("any",
                                std::make_shared<ASTIdentifier>("labels")));

            exp_list->children.push_back(
                makeASTFunction("groupArray",
                                makeASTFunction("tuple",
                                                std::make_shared<ASTIdentifier>("timestamp"), std::make_shared<ASTIdentifier>("value"))));

            select_query->setExpression(ASTSelectQuery::Expression::SELECT, exp_list);
        }

        /// FROM prometheus.time_series
        auto tables = std::make_shared<ASTTablesInSelectQuery>();

        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();
            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(time_series_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);
        }

        /// SEMI LEFT JOIN prometheus.labels ON prometheus.time_series.labels_hash = prometheus.labels.labels_hash
        {
            auto table = std::make_shared<ASTTablesInSelectQueryElement>();

            auto table_join = std::make_shared<ASTTableJoin>();
            table_join->kind = JoinKind::Left;
            table_join->strictness = JoinStrictness::Semi;

            table_join->on_expression =
                makeASTFunction("equals",
                                std::make_shared<ASTIdentifier>(Strings{time_series_table_id.database_name, time_series_table_id.table_name, "labels_hash"}),
                                std::make_shared<ASTIdentifier>(Strings{labels_table_id.database_name, labels_table_id.table_name, "labels_hash"}));
            table->table_join = table_join;

            auto table_exp = std::make_shared<ASTTableExpression>();
            table_exp->database_and_table_name = std::make_shared<ASTTableIdentifier>(labels_table_id);
            table_exp->children.emplace_back(table_exp->database_and_table_name);

            table->table_expression = table_exp;
            tables->children.push_back(table);

            select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);
        }

        /// WHERE <filter>
        if (auto where = makeASTFilterForReadingTimeSeries(start_timestamp_ms, end_timestamp_ms, label_matcher))
            select_query->setExpression(ASTSelectQuery::Expression::WHERE, std::move(where));

        /// GROUP BY labels_hash
        {
            auto exp_list = std::make_shared<ASTExpressionList>();
            exp_list->children.push_back(std::make_shared<ASTIdentifier>("labels_hash"));
            select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, exp_list);
        }

        return select_query;
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


void PrometheusStorage::reloadConfiguration(const Poco::Util::AbstractConfiguration & config_)
{
    try
    {
        loadConfigImpl(config_, /* mutex_already_locked= */ false);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to update configuration");
    }
}


void PrometheusStorage::loadConfig(const Poco::Util::AbstractConfiguration & config_)
{
    std::lock_guard lock{mutex};
    if (!config_loaded)
        loadConfigImpl(config_, /* mutex_already_locked= */ true);
}


void PrometheusStorage::loadConfigImpl(const Poco::Util::AbstractConfiguration & config_, bool mutex_already_locked)
{
    TableConfig new_config[static_cast<size_t>(TableKind::MAX)];

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        String config_section = getConfigSectionForTable(prometheus_storage_id, table_kind, config_);

        TableConfig table_config;
        table_config.database_name = config_.getString(config_section + ".database", DEFAULT_DATABASE_NAME);
        table_config.table_name = config_.getString(config_section + ".table", getDefaultTableName(table_kind));
        table_config.engine = config_.getString(config_section + ".engine", getDefaultTableEngine(table_kind));

        /// Validate engine definition syntax to prevent some configuration errors.
        ParserStorageWithComment storage_parser{ParserStorage::TABLE_ENGINE};

        parseQuery(storage_parser, table_config.engine.data(), table_config.engine.data() + table_config.engine.size(),
                   "Storage to create table " + table_config.table_name + "(prometheus storage " + prometheus_storage_id + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

        new_config[static_cast<size_t>(table_kind)] = table_config;
    }

    {
        std::unique_lock lock{mutex, std::defer_lock};
        if (!mutex_already_locked)
            lock.lock();

        bool config_changed = false;
        for (auto table_kind : collections::range(TableKind::MAX))
        {
            const auto & new_table_config = new_config[static_cast<size_t>(table_kind)];
            auto & table_config = config[static_cast<size_t>(table_kind)];
            if (table_config != new_table_config)
            {
                table_config = new_table_config;
                config_changed = true;
                if (config_loaded)
                    LOG_INFO(log, "Configuration changed");
            }
        }

        if (config_changed)
        {
            /// We need to check or recreate our table once again because the config has changed.
            create_queries_built = false;
            tables_created = false;
            for (auto table_kind : collections::range(TableKind::MAX))
            {
                create_queries[static_cast<size_t>(table_kind)] = nullptr;
                table_ids[static_cast<size_t>(table_kind)] = StorageID::createEmpty();
            }
        }

        config_loaded = true;
    }
}


void PrometheusStorage::buildCreateQueries(ContextPtr global_context)
{
    std::lock_guard lock{mutex};
    if (create_queries_built)
        return;

    for (auto table_kind : collections::range(TableKind::MAX))
    {
        const auto & table_config = config[static_cast<size_t>(table_kind)];
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

    create_queries_built = true;
}


void PrometheusStorage::createTables(ContextPtr global_context)
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
    loadConfig(global_context->getConfigRef());
    buildCreateQueries(global_context);
    createTables(global_context);
}


struct PrometheusStorage::BlocksToInsert
{
    Block blocks[static_cast<size_t>(TableKind::MAX)];
};


void PrometheusStorage::writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series, ContextPtr context)
{
    LOG_TRACE(log, "Writing {} time series", time_series.size());

    prepareTables(context->getGlobalContext());
    insertToTables(toBlocks(time_series), context);

    LOG_TRACE(log, "{} time series have been written", time_series.size());
}


void PrometheusStorage::writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata, ContextPtr context)
{
    LOG_TRACE(log, "Writing {} metrics metadata", metrics_metadata.size());

    prepareTables(context->getGlobalContext());
    insertToTables(toBlocks(metrics_metadata), context);

    LOG_TRACE(log, "{} metrics metadata has been written", metrics_metadata.size());
}


PrometheusStorage::BlocksToInsert PrometheusStorage::toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series) const
{
    BlocksToInsert res;

    size_t labels_num_new_rows = time_series.size();
    if (labels_num_new_rows)
        createColumns(res.blocks[static_cast<size_t>(TableKind::LABELS)], columns_descriptions[static_cast<size_t>(TableKind::LABELS)], labels_num_new_rows);

    size_t time_series_num_new_rows = 0;
    for (const auto & element : time_series)
        time_series_num_new_rows += element.samples_size();

    if (time_series_num_new_rows)
        createColumns(res.blocks[static_cast<size_t>(TableKind::TIME_SERIES)], columns_descriptions[static_cast<size_t>(TableKind::TIME_SERIES)], time_series_num_new_rows);

    ColumnUInt128 * time_series_column_labels_hash = nullptr;
    ColumnDecimal<DateTime64> * time_series_column_timestamp = nullptr;
    ColumnFloat64 * time_series_column_value = nullptr;

    ColumnString * labels_column_metric_name = nullptr;
    ColumnMap * labels_column_labels = nullptr;
    ColumnString * labels_column_labels_names = nullptr;
    ColumnString * labels_column_labels_values = nullptr;
    ColumnArray::Offsets * labels_column_labels_offsets = nullptr;
    ColumnUInt128 * labels_column_labels_hash = nullptr;

    auto assign_column_var = [](auto * & dest_column_ptr, const ColumnPtr & src_column)
    {
        dest_column_ptr = typeid_cast<std::remove_cvref_t<decltype(dest_column_ptr)>>(&src_column->assumeMutableRef());
    };

    auto assign_column_var_from_result = [&](auto * & dest_column_ptr, TableKind table_kind, size_t column_pos)
    {
        assign_column_var(dest_column_ptr, res.blocks[static_cast<size_t>(table_kind)].getByPosition(column_pos).column);
    };

    if (time_series_num_new_rows)
    {
        assign_column_var_from_result(time_series_column_labels_hash, TableKind::TIME_SERIES, 0);
        assign_column_var_from_result(time_series_column_timestamp,   TableKind::TIME_SERIES, 1);
        assign_column_var_from_result(time_series_column_value,       TableKind::TIME_SERIES, 2);
    }

    if (labels_num_new_rows)
    {
        assign_column_var_from_result(labels_column_metric_name, TableKind::LABELS, 0);
        assign_column_var_from_result(labels_column_labels,      TableKind::LABELS, 1);
        assign_column_var_from_result(labels_column_labels_hash, TableKind::LABELS, 2);

        assign_column_var(labels_column_labels_names, labels_column_labels->getNestedData().getColumnPtr(0));
        assign_column_var(labels_column_labels_values, labels_column_labels->getNestedData().getColumnPtr(1));
        labels_column_labels_offsets = &labels_column_labels->getNestedColumn().getOffsets();
    }

    for (const auto & element : time_series)
    {
        const auto & labels = element.labels();
        int name_label_pos = checkLabels(labels);
        auto labels_hash = calculateLabelsHash(labels);
        const auto & metric_name = labels[name_label_pos].value();

        labels_column_metric_name->insertData(metric_name.data(), metric_name.length());

        for (int i = 0; i != labels.size(); ++i)
        {
            const auto & label = labels[i];
            const auto & label_name = label.name();
            const auto & label_value = label.value();

            if (i != name_label_pos)
            {
                labels_column_labels_names->insertData(label_name.data(), label_name.length());
                labels_column_labels_values->insertData(label_value.data(), label_value.length());
            }
        }

        labels_column_labels_offsets->push_back(labels_column_labels_names->size());
        labels_column_labels_hash->insertValue(labels_hash);

        for (const auto & sample : element.samples())
        {
            time_series_column_labels_hash->insertValue(labels_hash);
            time_series_column_timestamp->insertValue(sample.timestamp());
            time_series_column_value->insertValue(sample.value());
        }
    }

    return res;
}


PrometheusStorage::BlocksToInsert PrometheusStorage::toBlocks(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata) const
{
    BlocksToInsert res;

    size_t metrics_metadata_num_new_rows = metrics_metadata.size();
    if (metrics_metadata_num_new_rows)
    {
        createColumns(res.blocks[static_cast<size_t>(TableKind::METRICS_METADATA)], columns_descriptions[static_cast<size_t>(TableKind::METRICS_METADATA)],
                      metrics_metadata_num_new_rows);
    }

    ColumnString * column_metric_family_name = nullptr;
    ColumnString * column_type = nullptr;
    ColumnString * column_help = nullptr;
    ColumnString * column_unit = nullptr;

    auto assign_column_var = [](auto * & dest_column_ptr, const ColumnPtr & src_column)
    {
        dest_column_ptr = typeid_cast<std::remove_cvref_t<decltype(dest_column_ptr)>>(&src_column->assumeMutableRef());
    };

    auto assign_column_var_from_result = [&](auto * & dest_column_ptr, TableKind table_kind, size_t column_pos)
    {
        assign_column_var(dest_column_ptr, res.blocks[static_cast<size_t>(table_kind)].getByPosition(column_pos).column);
    };

    if (metrics_metadata_num_new_rows)
    {
        assign_column_var_from_result(column_metric_family_name, TableKind::METRICS_METADATA, 0);
        assign_column_var_from_result(column_type,               TableKind::METRICS_METADATA, 1);
        assign_column_var_from_result(column_help,               TableKind::METRICS_METADATA, 2);
        assign_column_var_from_result(column_unit,               TableKind::METRICS_METADATA, 3);
    }

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


void PrometheusStorage::insertToTables(BlocksToInsert && blocks, ContextPtr context)
{
    StorageID table_ids_to_insert[static_cast<size_t>(TableKind::MAX)] = {StorageID::createEmpty(), StorageID::createEmpty(), StorageID::createEmpty()};
    {
        std::lock_guard lock{mutex};
        for (auto table_kind : collections::range(TableKind::MAX))
            table_ids_to_insert[static_cast<size_t>(table_kind)] = table_ids[static_cast<size_t>(table_kind)];
    }

    /// We use this specific order of insertion because we want our tables to be consistent even if some of the following insertion fail.
    /// That's why we insert to the "time_series" table after inserting to the "labels" table.
    TableKind insert_order[] = {TableKind::LABELS, TableKind::TIME_SERIES, TableKind::METRICS_METADATA};
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


void PrometheusStorage::readTimeSeries(google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & result_time_series,
                                       Int64 start_timestamp_ms,
                                       Int64 end_timestamp_ms,
                                       const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matchers,
                                       const prometheus::ReadHints & read_hints,
                                       ContextPtr context)
{
    LOG_TRACE(log, "Reading time series");

    result_time_series.Clear();

    prepareTables(context->getGlobalContext());

    ASTPtr select_query = buildSelectQueryForReadingTimeSeries(start_timestamp_ms, end_timestamp_ms, label_matchers, read_hints);

    LOG_TRACE(log, "Executing query {}", select_query);

    ContextMutablePtr select_context = Context::createCopy(context);
    select_context->setCurrentQueryId("");
    CurrentThread::QueryScope query_scope{select_context};

    InterpreterSelectQuery interpreter(select_query, select_context, SelectQueryOptions{});
    BlockIO io = interpreter.execute();
    PullingPipelineExecutor executor(io.pipeline);

    Block block;
    while (executor.pull(block))
    {
        LOG_INFO(&Poco::Logger::get("!!!"), "readTimeSeries: pulled block with {} columns and {} rows", block.columns(), block.rows());
        if (block)
            appendBlockToTimeSeries(result_time_series, std::move(block));
    }

    LOG_TRACE(log, "{} time series have been read", result_time_series.size());
}


ASTPtr PrometheusStorage::buildSelectQueryForReadingTimeSeries(
    Int64 start_timestamp_ms,
    Int64 end_timestamp_ms,
    const google::protobuf::RepeatedPtrField<prometheus::LabelMatcher> & label_matchers,
    const prometheus::ReadHints &)
{
    StorageID time_series_table_id = StorageID::createEmpty();
    StorageID labels_table_id = StorageID::createEmpty();
    {
        std::lock_guard lock{mutex};
        time_series_table_id = table_ids[static_cast<size_t>(TableKind::TIME_SERIES)];
        labels_table_id = table_ids[static_cast<size_t>(TableKind::LABELS)];
    }
    return buildSelectQueryForReadingTimeSeriesImpl(time_series_table_id, labels_table_id, start_timestamp_ms, end_timestamp_ms, label_matchers);
}


void PrometheusStorage::appendBlockToTimeSeries(google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & result_time_series, Block && block)
{
    ColumnString * column_metric_name = nullptr;

    ColumnMap * column_labels = nullptr;
    ColumnString * column_labels_label_name = nullptr;
    ColumnString * column_labels_label_value = nullptr;
    ColumnArray::Offsets * column_labels_offset = nullptr;

    ColumnArray * column_time_series = nullptr;
    ColumnDecimal<DateTime64> * column_time_series_timestamp = nullptr;
    ColumnFloat64 * column_time_series_value;
    ColumnArray::Offsets * column_time_series_offset = nullptr;

    auto assign_column_var = [](auto * & dest_column_ptr, const ColumnPtr & src_column)
    {
        dest_column_ptr = typeid_cast<std::remove_cvref_t<decltype(dest_column_ptr)>>(&src_column->assumeMutableRef());
    };

    auto assign_column_var_from_block = [&](auto * & dest_column_ptr, size_t column_pos)
    {
        assign_column_var(dest_column_ptr, block.getByPosition(column_pos).column);
    };

    assign_column_var_from_block(column_metric_name, 0);
    assign_column_var_from_block(column_labels, 1);
    assign_column_var_from_block(column_time_series, 2);

    assign_column_var(column_labels_label_name, column_labels->getNestedData().getColumnPtr(0));
    assign_column_var(column_labels_label_value, column_labels->getNestedData().getColumnPtr(1));
    column_labels_offset = &column_labels->getNestedColumn().getOffsets();

    assign_column_var(column_time_series_timestamp, typeid_cast<ColumnTuple &>(column_time_series->getData()).getColumnPtr(0));
    assign_column_var(column_time_series_value, typeid_cast<ColumnTuple &>(column_time_series->getData()).getColumnPtr(1));
    column_time_series_offset = &column_time_series->getOffsets();

    size_t num_rows = block.rows();

    std::vector<std::pair<std::string_view, std::string_view>> labels;
    std::vector<std::pair<Int64, Float64>> time_series;

    for (size_t i = 0; i != num_rows; ++i)
    {
        labels.clear();
        size_t labels_start_offset = (*column_labels_offset)[i - 1];
        size_t labels_end_offset = (*column_labels_offset)[i];
        labels.reserve(labels_end_offset - labels_start_offset + 1);
        labels.emplace_back("__name__", column_metric_name->getDataAt(i));
        for (size_t j = labels_start_offset; j != labels_end_offset; ++j)
            labels.emplace_back(column_labels_label_name->getDataAt(j), column_labels_label_value->getDataAt(j));
        sortLabels(labels);

        time_series.clear();
        size_t time_series_start_offset = (*column_time_series_offset)[i - 1];
        size_t time_series_end_offset = (*column_time_series_offset)[i];
        time_series.reserve(time_series_end_offset - time_series_start_offset);
        for (size_t j = time_series_start_offset; j != time_series_end_offset; ++j)
            time_series.emplace_back(column_time_series_timestamp->getElement(j), column_time_series_value->getElement(j));
        sortTimeSeries(time_series);

        auto & new_time_series = *result_time_series.Add();

        for (const auto & [label_name, label_value] : labels)
        {
            auto & new_label = *new_time_series.add_labels();
            new_label.set_name(label_name);
            new_label.set_value(label_value);
        }
        
        for (const auto & [timestamp, value] : time_series)
        {
            auto & new_sample = *new_time_series.add_samples();
            new_sample.set_timestamp(timestamp);
            new_sample.set_value(value);
        }
    }
}

}
