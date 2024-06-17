#include <Storages/StorageTimeSeries.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>
#include <Storages/TimeSeries/TimeSeriesInnerTablesCreator.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>

#include <base/insertAtEnd.h>
#include <filesystem>


namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_QUERY;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


namespace
{
    using TargetKind = ViewTarget::Kind;
    namespace fs = std::filesystem;

    /// Loads TimeSeries storage settings from a create query.
    std::shared_ptr<const TimeSeriesSettings> getTimeSeriesSettingsFromQuery(const ASTCreateQuery & query)
    {
        auto storage_settings = std::make_shared<TimeSeriesSettings>();
        if (query.storage)
            storage_settings->loadFromQuery(*query.storage);
        return storage_settings;
    }

    /// Creates an inner target table or just makes its storage ID.
    /// This function is used by the constructor of StorageTimeSeries to find (or create) its target tables.
    std::pair<StorageID, bool /* inner */> initTarget(
        const ViewTarget & target_info,
        const ContextPtr & context,
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        LoadingStrictnessLevel mode)
    {
        bool is_inner = target_info.table_id.empty();
        StorageID res = StorageID::createEmpty();

        if (!is_inner)
        {
            /// A target table is specified.
            res = target_info.table_id;

            if (mode < LoadingStrictnessLevel::ATTACH)
            {
                /// If it's not an ATTACH request then
                /// check that the specified target table has all the required columns.
                auto target_table = DatabaseCatalog::instance().getTable(target_info.table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr();
                const auto & target_columns = target_metadata->columns;
                TimeSeriesColumnsValidator target_validator{target_info.table_id};
                target_validator.validateTargetColumns(target_info.kind, target_columns, time_series_settings);
            }
        }
        else
        {
            TimeSeriesInnerTablesCreator inner_tables_creator{time_series_storage_id};

            /// An inner target table should be used.
            if (mode >= LoadingStrictnessLevel::ATTACH)
            {
                /// If it's an ATTACH request, then the inner target table must be already created.
                res = inner_tables_creator.getInnerTableId(target_info);
            }
            else
            {
                /// Create the inner target table.
                res = inner_tables_creator.createInnerTable(target_info, context, time_series_columns, time_series_settings);
            }
        }

        return {res, is_inner};
    }
}


void StorageTimeSeries::validateTableStructure(ColumnsDescription & columns, const ASTCreateQuery & create_query)
{
    auto time_series_settings = getTimeSeriesSettingsFromQuery(create_query);
    StorageID time_series_storage_id{create_query.getDatabase(), create_query.getTable()};
    TimeSeriesColumnsValidator validator{time_series_storage_id};
    validator.validateColumns(columns, *time_series_settings);
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

    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    if (!comment.empty())
        storage_metadata.setComment(comment);
    setInMemoryMetadata(storage_metadata);

    has_inner_tables = false;

    for (auto target_kind : {TargetKind::Data, TargetKind::Tags, TargetKind::Metrics})
    {
        auto & target = targets.emplace_back();
        target.kind = target_kind;
        std::tie(target.table_id, target.has_inner_table) = initTarget(query.getTarget(target_kind), local_context, getStorageID(), columns, *storage_settings, mode);
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


StorageID StorageTimeSeries::getTargetTableId(TargetKind target_kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == target_kind)
            return target.table_id;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected target kind {}", toString(target_kind));
}

StoragePtr StorageTimeSeries::getTargetTable(TargetKind target_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().getTable(getTargetTableId(target_kind), local_context);
}

StoragePtr StorageTimeSeries::tryGetTargetTable(TargetKind target_kind, const ContextPtr & local_context) const
{
    return DatabaseCatalog::instance().tryGetTable(getTargetTableId(target_kind), local_context);
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
