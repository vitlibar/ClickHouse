#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTargetTables.h>
#include <Parsers/parseQuery.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <base/insertAtEnd.h>
#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
}


namespace
{
    namespace fs = std::filesystem;

    ColumnsDescription getColumnsDescription(const TimeSeriesSettings & time_series_settings)
    {
        return ColumnsDescription{
            { "id",                 DataTypeFactory::instance().get(time_series_settings.id_type)                                                   },
            { "metric_name",        std::make_shared<DataTypeString>()                                                                          },
            { "tags",               std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())       },
            { "time_series",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                                        DataTypes{DataTypeFactory::instance().get(time_series_settings.timestamp_type),
                                                  DataTypeFactory::instance().get(time_series_settings.value_type)},
                                        Strings{"timestamp", "value"})) },
            { "metric_family_name", std::make_shared<DataTypeString>() },
            { "type",               std::make_shared<DataTypeString>() },
            { "unit",               std::make_shared<DataTypeString>() },
            { "help",               std::make_shared<DataTypeString>() },
        };
    };

    ASTPtr parseCodec(const String & codec)
    {
        if (codec.empty())
            return nullptr;
        ParserCodec codec_parser;
        return parseQuery(codec_parser, "(" + codec + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }

    ColumnsDescription getInnerTableColumnsDescription(TargetTableKind inner_table_kind, const TimeSeriesSettings & time_series_settings)
    {
        switch (inner_table_kind)
        {
            case TargetTableKind::kData:
            {
                ColumnsDescription columns;
                columns.add(ColumnDescription{"id",        DataTypeFactory::instance().get(time_series_settings.id_type),        parseCodec(time_series_settings.id_codec),        ""});
                columns.add(ColumnDescription{"timestamp", DataTypeFactory::instance().get(time_series_settings.timestamp_type), parseCodec(time_series_settings.timestamp_codec), ""});
                columns.add(ColumnDescription{"value",     DataTypeFactory::instance().get(time_series_settings.value_type),     parseCodec(time_series_settings.value_codec),     ""});
                return columns;
            }

            case TargetTableKind::kMetrics:
            {
                ColumnsDescription columns;
                columns.add(ColumnDescription{"id",          DataTypeFactory::instance().get(time_series_settings.id_type)});
                columns.add(ColumnDescription{"metric_name", std::make_shared<DataTypeString>()});

                const Map & tags_to_columns = time_series_settings.tags_to_columns;
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    columns.add(ColumnDescription{column_name, std::make_shared<DataTypeString>()});
                }

                columns.add(ColumnDescription{"tags", std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())});
                return columns;
            }

            case TargetTableKind::kMetadata:
            {
                return ColumnsDescription{
                    { "metric_family_name", std::make_shared<DataTypeString>() },
                    { "type",               std::make_shared<DataTypeString>() },
                    { "unit",               std::make_shared<DataTypeString>() },
                    { "help",               std::make_shared<DataTypeString>() },
                };
            }

            default:
                chassert(false);
        }
    }

    StorageID generateInnerTableId(const StorageID & time_series_table_id, TargetTableKind inner_table_kind, const UUID & inner_uuid)
    {
        StorageID res = time_series_table_id;
        String table_name;
        if (time_series_table_id.hasUUID())
            res.table_name = fmt::format(".inner_id.{}.{}", toString(inner_table_kind), time_series_table_id.uuid);
        else
            res.table_name = fmt::format(".inner.{}.{}", toString(inner_table_kind), time_series_table_id.table_name);
        res.uuid = inner_uuid;
        return res;
    }

    std::shared_ptr<ASTStorage> getDefaultEngineForInnerTable(TargetTableKind inner_table_kind)
    {
        auto storage = std::make_shared<ASTStorage>();
        switch (inner_table_kind)
        {
            case TargetTableKind::kData:
            {
                storage->set(storage->engine, makeASTFunction("MergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->order_by, makeASTFunction("tuple", std::make_shared<ASTIdentifier>("id"), std::make_shared<ASTIdentifier>("timestamp")));
                break;
            }
            case TargetTableKind::kMetrics:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->primary_key, std::make_shared<ASTIdentifier>("metric_name"));
                storage->set(storage->order_by, makeASTFunction("tuple", std::make_shared<ASTIdentifier>("metric_name"), std::make_shared<ASTIdentifier>("id")));
                break;
            }
            case TargetTableKind::kMetadata:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->order_by, std::make_shared<ASTIdentifier>("metric_family_name"));
                break;
            }
            default:
                chassert(false);
        }
        return storage;
    }

    std::shared_ptr<ASTCreateQuery> getCreateQueryForInnerTable(
        const StorageID & time_series_table_id,
        const TimeSeriesSettings & time_series_settings,
        TargetTableKind inner_table_kind,
        const UUID & inner_uuid,
        const ASTStorage * inner_storage)
    {
        auto create = std::make_shared<ASTCreateQuery>();

        auto inner_table_id = generateInnerTableId(time_series_table_id, inner_table_kind, inner_uuid);
        create->setDatabase(inner_table_id.database_name);
        create->setTable(inner_table_id.table_name);
        create->uuid = inner_table_id.uuid;

        auto new_columns_list = std::make_shared<ASTColumns>();
        new_columns_list->set(new_columns_list->columns,
                              InterpreterCreateQuery::formatColumns(getInnerTableColumnsDescription(inner_table_kind, time_series_settings)));
        create->set(create->columns_list, new_columns_list);

        if (inner_storage)
        {
            create->set(create->storage, inner_storage->clone());

            /// Set ORDER BY if not set.
            if (!create->storage->order_by && !create->storage->primary_key && create->storage->engine && create->storage->engine->name.ends_with("MergeTree"))
            {
                auto default_engine = getDefaultEngineForInnerTable(inner_table_kind);
                if (default_engine->order_by)
                    create->storage->set(create->storage->order_by, default_engine->order_by->clone());
                if (default_engine->primary_key)
                    create->storage->set(create->storage->primary_key, default_engine->primary_key->clone());
            }
        }
        else
        {
            create->set(create->storage, getDefaultEngineForInnerTable(inner_table_kind));
        }

        return create;
    }

    void initTargetTable(
        TargetTableKind target_kind,
        StorageID & out_table_id,
        bool & out_has_inner_table,
        const ASTCreateQuery & query,
        const StorageID & time_series_table_id,
        const TimeSeriesSettings & time_series_settings,
        const ContextPtr & context,
        LoadingStrictnessLevel mode)
    {
        const auto & target = query.getTarget(target_kind);
        out_has_inner_table = target.table_id.empty();

        if (!out_has_inner_table)
        {
            out_table_id = target.table_id;
        }
        else if (LoadingStrictnessLevel::ATTACH <= mode)
        {
            /// If there is an ATTACH request, then the internal table must already be created.
            out_table_id = generateInnerTableId(time_series_table_id, target_kind, target.inner_uuid);
        }
        else
        {
            /// We will create a query to create an internal table.
            auto create_context = Context::createCopy(context);

            auto manual_create_query = getCreateQueryForInnerTable(
                time_series_table_id, time_series_settings, target_kind, target.inner_uuid, target.storage);

            InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
            create_interpreter.setInternal(true);
            create_interpreter.execute();

            out_table_id = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, context)->getStorageID();
        }
    }

}


StorageTimeSeries::StorageTimeSeries(
    const StorageID & table_id,
    const ContextPtr & local_context,
    LoadingStrictnessLevel mode,
    const ASTCreateQuery & query,
    const ColumnsDescription & columns,
    const String & comment)
    : IStorage(table_id)
    , WithContext(local_context->getGlobalContext())
{
    if (!columns.empty())
        throw Exception{ErrorCodes::INCORRECT_QUERY, "The {} table engine doesn't allow specifying custom columns in the CREATE query", getName()};

    if (query.storage)
        storage_settings.loadFromQuery(*query.storage);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(getColumnsDescription(storage_settings));
    if (!comment.empty())
        storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    targets.emplace_back(TargetTableKind::kData);
    initTargetTable(TargetTableKind::kData, targets.back().table_id, targets.back().has_inner_table, query, getStorageID(), storage_settings, local_context, mode);

    targets.emplace_back(TargetTableKind::kMetrics);
    initTargetTable(TargetTableKind::kMetrics, targets.back().table_id, targets.back().has_inner_table, query, getStorageID(), storage_settings, local_context, mode);

    targets.emplace_back(TargetTableKind::kMetadata);
    initTargetTable(TargetTableKind::kMetadata, targets.back().table_id, targets.back().has_inner_table, query, getStorageID(), storage_settings, local_context, mode);

    has_inner_tables = false;
    for (const auto & target : targets)
        has_inner_tables |= target.has_inner_table;
}


void StorageTimeSeries::startup()
{
}

void StorageTimeSeries::shutdown(bool)
{
}


void StorageTimeSeries::drop()
{
    /// Sync flag and the setting make sense for Atomic databases only.
    /// However, with Atomic databases, IStorage::drop() can be called only from a background task in DatabaseCatalog.
    /// Running synchronous DROP from that task leads to deadlock.
    dropInnerTableIfAny(/* sync= */ false, getContext());
}

void StorageTimeSeries::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    for (const auto & target : targets)
    {
        if (target.has_inner_table && DatabaseCatalog::instance().tryGetTable(target.table_id, getContext()))
        {
            /// Best-effort to make them work: the inner table name is almost always less than the TimeSeries name (so it's safe to lock DDLGuard).
            /// (See the comment in StorageMaterializedView::dropInnerTableIfAny.)
            bool may_lock_ddl_guard = getStorageID().getQualifiedName() < target.table_id.getQualifiedName();
            InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, getContext(), local_context, target.table_id,
                                                sync, /* ignore_sync_setting= */ true, may_lock_ddl_guard);
        }
    }
}

void StorageTimeSeries::truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr local_context, TableExclusiveLockHolder &)
{
    for (const auto & target : targets)
    {
        /// We truncate only inner tables here.
        if (target.has_inner_table)
            InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, target.table_id, /* sync= */ true);
    }
}


StorageID StorageTimeSeries::getTargetTableId(TargetTableKind target_kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == target_kind)
            return target.table_id;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", toString(target_kind));
}

StoragePtr StorageTimeSeries::getTargetTable(TargetTableKind table_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().getTable(getTargetTableId(table_kind), local_context);
}

StoragePtr StorageTimeSeries::tryGetTargetTable(TargetTableKind table_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().tryGetTable(getTargetTableId(table_kind), local_context);
}


std::optional<UInt64> StorageTimeSeries::totalRows(const Settings & settings) const
{
    UInt64 total_rows = 0;
    for (const auto & target : targets)
    {
        if (target.has_inner_table)
        {
            auto inner_table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
            if (!inner_table)
                return std::nullopt;

            auto total_rows_in_inner_table = inner_table->totalRows(settings);
            if (!total_rows_in_inner_table)
                return std::nullopt;

            total_rows += *total_rows_in_inner_table;
        }
    }
    return total_rows;
}

std::optional<UInt64> StorageTimeSeries::totalBytes(const Settings & settings) const
{
    UInt64 total_bytes = 0;
    for (const auto & target : targets)
    {
        if (target.has_inner_table)
        {
            auto inner_table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytes(settings);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

std::optional<UInt64> StorageTimeSeries::totalBytesUncompressed(const Settings & settings) const
{
    UInt64 total_bytes = 0;
    for (const auto & target : targets)
    {
        if (target.has_inner_table)
        {
            auto inner_table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
            if (!inner_table)
                return std::nullopt;

            auto total_bytes_in_inner_table = inner_table->totalBytesUncompressed(settings);
            if (!total_bytes_in_inner_table)
                return std::nullopt;

            total_bytes += *total_bytes_in_inner_table;
        }
    }
    return total_bytes;
}

Strings StorageTimeSeries::getDataPaths() const
{
    Strings data_paths;
    for (const auto & target : targets)
    {
        auto table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
        if (!table)
            continue;

        insertAtEnd(data_paths, table->getDataPaths());
    }
    return data_paths;
}


bool StorageTimeSeries::optimize(
    const ASTPtr & query,
    const StorageMetadataPtr &,
    const ASTPtr & partition,
    bool final,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    ContextPtr local_context)
{
    if (!has_inner_tables)
    {
        throw Exception(ErrorCodes::INCORRECT_QUERY, "TimeSeries table {} targets only existing tables. Execute the statement directly on it.",
                        getStorageID().getNameForLogs());
    }

    bool optimized = false;
    for (const auto & target : targets)
    {
        if (target.has_inner_table)
        {
            auto inner_table = DatabaseCatalog::instance().getTable(target.table_id, local_context);
            optimized |= inner_table->optimize(query, inner_table->getInMemoryMetadataPtr(), partition, final, deduplicate, deduplicate_by_columns, cleanup, local_context);
        }
    }

    return optimized;
}


void StorageTimeSeries::checkAlterIsPossible(const AlterCommands & commands, ContextPtr) const
{
    for (const auto & command : commands)
    {
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void StorageTimeSeries::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder & table_lock_holder)
{
    IStorage::alter(params, local_context, table_lock_holder);
}


void StorageTimeSeries::renameInMemory(const StorageID & new_table_id)
{
    UNUSED(new_table_id);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Renaming is not supported by storage {} yet", getName());
}


void StorageTimeSeries::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    for (const auto & target : targets)
    {
        /// We backup the target table's data only if it's inner.
        if (target.has_inner_table)
        {
            auto table = DatabaseCatalog::instance().getTable(target.table_id, getContext());
            table->backupData(backup_entries_collector, fs::path{data_path_in_backup} / toString(target.kind), {});
        }
    }
}

void StorageTimeSeries::restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    for (const auto & target : targets)
    {
        /// We backup the target table's data only if it's inner.
        if (target.has_inner_table)
        {
            auto table = DatabaseCatalog::instance().getTable(target.table_id, getContext());
            table->restoreDataFromBackup(restorer, fs::path{data_path_in_backup} / toString(target.kind), {});
        }
    }
}


void StorageTimeSeries::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    UNUSED(query_plan);
    UNUSED(column_names);
    UNUSED(storage_snapshot);
    UNUSED(query_info);
    UNUSED(local_context);
    UNUSED(processed_stage);
    UNUSED(max_block_size);
    UNUSED(num_streams);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SELECT is not supported by storage {} yet", getName());
}


SinkToStoragePtr StorageTimeSeries::write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr local_context, bool async_insert)
{
    UNUSED(query);
    UNUSED(metadata_snapshot);
    UNUSED(local_context);
    UNUSED(async_insert);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "INSERT is not supported by storage {} yet", getName());
}


void registerStorageTimeSeries(StorageFactory & factory)
{
    factory.registerStorage("TimeSeries", [](const StorageFactory::Arguments & args)
    {
        /// Pass local_context here to convey setting to inner tables.
        return std::make_shared<StorageTimeSeries>(
            args.table_id, args.getLocalContext(), args.mode, args.query, args.columns, args.comment);
    }
    ,
    {
        .supports_settings = true,
        .supports_schema_inference = true,
    });
}

}
