#include <Storages/StorageTimeSeries.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTargetTables.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesIDCalculator.h>
#include <Storages/TimeSeries/getTimeSeriesTableActualStructure.h>

#include <base/insertAtEnd.h>
#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_TABLE_ENGINE;
    extern const int INCORRECT_QUERY;
    extern const int UNEXPECTED_TABLE_ENGINE;
}


namespace
{
    namespace fs = std::filesystem;

    /// Loads the TimeSeries storage settings from a create query.
    std::shared_ptr<const TimeSeriesSettings> getTimeSeriesSettingsFromQuery(const ASTCreateQuery & query)
    {
        auto storage_settings = std::make_shared<TimeSeriesSettings>();
        if (query.storage)
            storage_settings->loadFromQuery(*query.storage);
        return storage_settings;
    }

    /// Generates a storage id for an inner target table.
    StorageID generateInnerTableId(const StorageID & time_series_storage_id, TargetTableKind inner_table_kind, const UUID & inner_uuid)
    {
        StorageID res = time_series_storage_id;
        String table_name;
        if (time_series_storage_id.hasUUID())
            res.table_name = fmt::format(".inner_id.{}.{}", toString(inner_table_kind), time_series_storage_id.uuid);
        else
            res.table_name = fmt::format(".inner.{}.{}", toString(inner_table_kind), time_series_storage_id.table_name);
        res.uuid = inner_uuid;
        return res;
    }

    /// Makes description of the columns of an inner target table.
    ColumnsDescription getInnerTableColumnsDescription(
        TargetTableKind inner_table_kind, const ColumnsDescription & time_series_columns, const TimeSeriesSettings & time_series_settings)
    {
        ColumnsDescription columns;

        switch (inner_table_kind)
        {
            case TargetTableKind::kData:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kID));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kTimestamp));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kValue));
                break;
            }

            case TargetTableKind::kTags:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kID));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kMetricName));

                /// We put known tags to the columns specified in the storage settings.
                const Map & tags_to_columns = time_series_settings.tags_to_columns;
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    columns.add(time_series_columns.get(column_name));
                }

                /// We put other tags to "tags" column.
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kTags));
                break;
            }

            case TargetTableKind::kMetrics:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kMetricFamilyName));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kType));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kUnit));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::kHelp));
                break;
            }

            default:
                chassert(false);
        }

        {
            const Map & extra_columns = time_series_settings.extra_columns;
            for (const auto & column_name_and_table : extra_columns)
            {
                const auto & tuple = column_name_and_table.safeGet<const Tuple &>();
                const auto & column_name = tuple.at(0).safeGet<String>();
                const auto & inner_target = tuple.at(1).safeGet<String>();
                if (inner_target == toString(inner_table_kind))
                    columns.add(time_series_columns.get(column_name));
            }
        }

        return columns;
    }

    /// Makes description of the default table engine of an inner target table.
    std::shared_ptr<ASTStorage> getDefaultEngineForInnerTable(TargetTableKind inner_table_kind)
    {
        auto storage = std::make_shared<ASTStorage>();
        switch (inner_table_kind)
        {
            case TargetTableKind::kData:
            {
                storage->set(storage->engine, makeASTFunction("MergeTree"));
                storage->engine->no_empty_args = false;

                storage->set(storage->order_by,
                             makeASTFunction("tuple",
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kID),
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kTimestamp)));
                break;
            }
            case TargetTableKind::kTags:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;

                storage->set(storage->primary_key,
                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kMetricName));

                storage->set(storage->order_by,
                             makeASTFunction("tuple",
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kMetricName),
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kID)));
                break;
            }
            case TargetTableKind::kMetrics:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->order_by, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::kMetricFamilyName));
                break;
            }
            default:
                chassert(false);
        }
        return storage;
    }

    /// Generates a CREATE query for creating an inner target table.
    std::shared_ptr<ASTCreateQuery> getCreateQueryForInnerTable(
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        TargetTableKind inner_table_kind,
        const UUID & inner_uuid,
        const ASTStorage * inner_storage)
    {
        auto create = std::make_shared<ASTCreateQuery>();

        auto inner_table_id = generateInnerTableId(time_series_storage_id, inner_table_kind, inner_uuid);
        create->setDatabase(inner_table_id.database_name);
        create->setTable(inner_table_id.table_name);
        create->uuid = inner_table_id.uuid;

        auto new_columns_list = std::make_shared<ASTColumns>();
        create->set(create->columns_list, new_columns_list);
        new_columns_list->set(
            new_columns_list->columns,
            InterpreterCreateQuery::formatColumns(
                getInnerTableColumnsDescription(inner_table_kind, time_series_columns, time_series_settings)));

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

    /// Creates an inner target table or just makes its storage ID.
    /// This function is used by the constructor of StorageTimeSeries to find (or create) its target tables.
    std::pair<StorageID, bool /* inner */> initTarget(
        TargetTableKind target_kind,
        const ASTCreateQuery & query,
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        const ContextPtr & context,
        LoadingStrictnessLevel mode)
    {
        const auto & target = query.getTarget(target_kind);
        bool inner = target.table_id.empty();
        StorageID res = StorageID::createEmpty();

        if (!inner)
        {
            res = target.table_id;
        }
        else if (LoadingStrictnessLevel::ATTACH <= mode)
        {
            /// If there is an ATTACH request, then the internal table must already be created.
            res = generateInnerTableId(time_series_storage_id, target_kind, target.inner_uuid);
        }
        else
        {
            /// We will create a query to create an internal table.
            auto create_context = Context::createCopy(context);

            auto manual_create_query = getCreateQueryForInnerTable(time_series_storage_id, time_series_columns, time_series_settings, target_kind, target.inner_uuid, target.storage);

            InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
            create_interpreter.setInternal(true);
            create_interpreter.execute();

            res = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, context)->getStorageID();
        }

        return {res, inner};
    }
}


ColumnsDescription StorageTimeSeries::getActualStructure(
    const ContextPtr & local_context, const ASTCreateQuery & create_query, const ColumnsDescription & columns_from_create_query)
{
    return getTimeSeriesTableActualStructure(local_context, create_query, columns_from_create_query);
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
    storage_settings = getTimeSeriesSettingsFromQuery(query);

    id_calculator = TimeSeriesIDCalculatorFactory::instance().create(storage_settings->id_algorithm);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    if (!comment.empty())
        storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    id_calculator = TimeSeriesIDCalculatorFactory::instance().create(storage_settings->id_algorithm);

    has_inner_tables = false;

    for (auto target_kind : {TargetTableKind::kData, TargetTableKind::kTags, TargetTableKind::kMetrics})
    {
        auto & target = targets.emplace_back(target_kind);
        std::tie(target.table_id, target.has_inner_table) = initTarget(target_kind, query, getStorageID(), columns, *storage_settings, local_context, mode);
        has_inner_tables |= target.has_inner_table;
    }
}


StorageTimeSeries::~StorageTimeSeries() = default;


TimeSeriesSettings StorageTimeSeries::getStorageSettings() const
{
    return *getStorageSettingsPtr();
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


std::shared_ptr<StorageTimeSeries> storagePtrToTimeSeries(StoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
}

std::shared_ptr<const StorageTimeSeries> storagePtrToTimeSeries(ConstStoragePtr storage)
{
    if (auto res = typeid_cast<std::shared_ptr<const StorageTimeSeries>>(storage))
        return res;

    throw Exception(
        ErrorCodes::UNEXPECTED_TABLE_ENGINE,
        "This operation can be executed on a TimeSeries table only, the engine of table {} is not TimeSeries",
        storage->getStorageID().getNameForLogs());
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
