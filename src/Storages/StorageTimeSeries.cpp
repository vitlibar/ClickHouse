#include <Storages/StorageTimeSeries.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/checkTimeSeriesTargetTable.h>
#include <Storages/TimeSeries/createTimeSeriesInnerTable.h>
#include <Storages/TimeSeries/normalizeTimeSeriesDefinition.h>

#include <base/insertAtEnd.h>
#include <filesystem>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_table;
}

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int UNEXPECTED_TABLE_ENGINE;
}

namespace fs = std::filesystem;


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
    if (mode <= LoadingStrictnessLevel::CREATE && !local_context->getSettingsRef()[Setting::allow_experimental_time_series_table])
    {
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Experimental TimeSeries table engine "
                        "is not enabled (the setting 'allow_experimental_time_series_table')");
    }

    auto normalized_settings = std::make_shared<TimeSeriesSettings>();
    if (query.storage)
        normalized_settings->loadFromQuery(*query.storage);
    normalizeTimeSeriesSettings(*normalized_settings, query, local_context);
    storage_settings = normalized_settings;

    auto normalized_columns = columns;
    normalizeTimeSeriesColumns(normalized_columns, *storage_settings);

    /// Store if we need startup() to persist the normalized columns in the database.
    columns_changed_in_constructor = (normalized_columns != columns);

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(normalized_columns);
    if (!comment.empty())
        storage_metadata.setComment(comment);

    /// Initialize settings_changes from the fully resolved settings so that
    /// ALTER TABLE MODIFY/RESET SETTINGS can update and remove individual settings.
    if (query.storage && query.storage->settings)
        storage_metadata.settings_changes = query.storage->settings->clone();

    setInMemoryMetadata(storage_metadata);

    /// Store a clone of the CREATE TABLE query so ALTER TABLE MODIFY/RESET SETTINGS
    /// can pass it to getNormalizedTimeSeriesSettings to re-derive dependent defaults.
    create_query = query.clone();

    for (auto target_kind : {ViewTarget::Samples, ViewTarget::Tags, ViewTarget::Metrics})
    {
        Target target;
        target.kind = target_kind;

        if (auto target_table_id = query.getTargetTableID(target_kind))
        {
            /// A target table is specified.
            target.table_id = target_table_id;

            if (mode < LoadingStrictnessLevel::ATTACH)
            {
                /// If it's not an ATTACH request then
                /// check that the specified target table has all the required columns.
                auto target_table = DatabaseCatalog::instance().getTable(target_table_id, local_context);
                auto target_metadata = target_table->getInMemoryMetadataPtr();
                const auto & target_columns = target_metadata->columns;
                checkTimeSeriesTargetTable(target_table_id, target_columns, target_kind, *storage_settings);
            }
        }
        else
        {
            /// An inner target table should be used.
            target.is_inner_table = true;
            has_inner_tables = true;
            auto inner_uuid = query.getTargetInnerUUID(target_kind);

            if (mode >= LoadingStrictnessLevel::ATTACH)
            {
                /// If it's an ATTACH request, then the inner target table must be already created.
                target.table_id = getTimeSeriesInnerTableID(target_kind, inner_uuid, table_id);
            }
            else
            {
                /// Create the inner target table.
                auto inner_engine = getTimeSeriesInnerEngine(target_kind, query, *storage_settings, local_context);
                target.table_id = createTimeSeriesInnerTable(target_kind, inner_uuid, inner_engine, table_id, normalized_columns, *storage_settings, local_context);
            }
        }

        targets.emplace_back(std::move(target));
    }
}


StorageTimeSeries::~StorageTimeSeries() = default;


const TimeSeriesSettings & StorageTimeSeries::getStorageSettings() const
{
    return *storage_settings;
}

void StorageTimeSeries::startup()
{
    if (columns_changed_in_constructor)
    {
        auto time_series_table_id = getStorageID();
        StorageInMemoryMetadata current_metadata = getInMemoryMetadata();
        DatabaseCatalog::instance().getDatabase(time_series_table_id.database_name)->alterTable(
            getContext(), time_series_table_id, current_metadata, /*validate_new_create_query=*/false);
    }
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
    if (!has_inner_tables)
        return;

    for (const auto & target : targets)
    {
        if (target.is_inner_table && DatabaseCatalog::instance().tryGetTable(target.table_id, getContext()))
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
    if (!has_inner_tables)
        return;

    for (const auto & target : targets)
    {
        /// We truncate only inner tables here.
        if (target.is_inner_table)
            InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Truncate, getContext(), local_context, target.table_id, /* sync= */ true);
    }
}


StorageID StorageTimeSeries::getTargetTableId(ViewTarget::Kind target_kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == target_kind)
            return target.table_id;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", toString(target_kind));
}

StoragePtr StorageTimeSeries::getTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().getTable(getTargetTableId(target_kind), local_context);
}

StoragePtr StorageTimeSeries::tryGetTargetTable(ViewTarget::Kind target_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().tryGetTable(getTargetTableId(target_kind), local_context);
}


std::optional<UInt64> StorageTimeSeries::totalRows(ContextPtr query_context) const
{
    UInt64 total_rows = 0;
    if (has_inner_tables)
    {
        for (const auto & target : targets)
        {
            if (target.is_inner_table)
            {
                auto inner_table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
                if (!inner_table)
                    return std::nullopt;

                auto total_rows_in_inner_table = inner_table->totalRows(query_context);
                if (!total_rows_in_inner_table)
                    return std::nullopt;

                total_rows += *total_rows_in_inner_table;
            }
        }
    }
    return total_rows;
}

std::optional<UInt64> StorageTimeSeries::totalBytes(ContextPtr query_context) const
{
    UInt64 total_bytes = 0;
    if (has_inner_tables)
    {
        for (const auto & target : targets)
        {
            if (target.is_inner_table)
            {
                auto inner_table = DatabaseCatalog::instance().tryGetTable(target.table_id, getContext());
                if (!inner_table)
                    return std::nullopt;

                auto total_bytes_in_inner_table = inner_table->totalBytes(query_context);
                if (!total_bytes_in_inner_table)
                    return std::nullopt;

                total_bytes += *total_bytes_in_inner_table;
            }
        }
    }
    return total_bytes;
}

std::optional<UInt64> StorageTimeSeries::totalBytesUncompressed(const Settings & settings) const
{
    UInt64 total_bytes = 0;
    if (has_inner_tables)
    {
        for (const auto & target : targets)
        {
            if (target.is_inner_table)
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
        if (target.is_inner_table)
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
        if (!command.isCommentAlter() && command.type != AlterCommand::MODIFY_SQL_SECURITY
            && !command.isSettingsAlter())
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Alter of type '{}' is not supported by storage {}", command.type, getName());
    }
}

void StorageTimeSeries::alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder &)
{
    StorageInMemoryMetadata new_metadata = getInMemoryMetadata();
    params.apply(new_metadata, local_context);

    bool has_settings_changes = std::any_of(
        params.begin(), params.end(), [](const AlterCommand & c) { return c.isSettingsAlter(); });

    if (has_settings_changes)
    {
        /// Clone the stored CREATE TABLE query, replace its settings, and use getNormalizedTimeSeriesSettings()
        /// to re-derive all dependent defaults (e.g. id_generator depends on id_type).
        auto new_create_query = boost::static_pointer_cast<ASTCreateQuery>(create_query->clone());
        chassert(new_create_query->storage);
        new_create_query->storage->set(new_create_query->storage->settings, new_metadata.settings_changes->clone());

        auto new_settings = std::make_shared<TimeSeriesSettings>(
            getNormalizedTimeSeriesSettings(*new_create_query, local_context));

        ColumnsDescription new_columns = new_metadata.getColumns();
        normalizeTimeSeriesColumns(new_columns, *new_settings);
        new_metadata.setColumns(new_columns);

        storage_settings = std::move(new_settings);
        create_query = std::move(new_create_query);
    }

    auto time_series_table_id = getStorageID();
    DatabaseCatalog::instance().getDatabase(time_series_table_id.database_name)->alterTable(
        local_context, time_series_table_id, new_metadata, /*validate_new_create_query=*/true);
    setInMemoryMetadata(new_metadata);
}


void StorageTimeSeries::renameInMemory(const StorageID & /* new_table_id */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Renaming is not supported by storage {} yet", getName());
}


void StorageTimeSeries::backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> &)
{
    for (const auto & target : targets)
    {
        /// We backup the target table's data only if it's inner.
        if (target.is_inner_table)
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
        if (target.is_inner_table)
        {
            auto table = DatabaseCatalog::instance().getTable(target.table_id, getContext());
            table->restoreDataFromBackup(restorer, fs::path{data_path_in_backup} / toString(target.kind), {});
        }
    }
}


void StorageTimeSeries::read(
    QueryPlan & /* query_plan */,
    const Names & /* column_names */,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & /* query_info */,
    ContextPtr /* local_context */,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "SELECT is not supported by storage {} yet", getName());
}


SinkToStoragePtr StorageTimeSeries::write(
    const ASTPtr & /* query */, const StorageMetadataPtr & /* metadata_snapshot */, ContextPtr /* local_context */, bool /* async_insert */)
{
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
        .has_builtin_setting_fn = TimeSeriesSettings::hasBuiltin,
    });
}

}
