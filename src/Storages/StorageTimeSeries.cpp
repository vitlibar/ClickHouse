#include <Storages/StorageTimeSeries.h>

#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTViewTargets.h>
#include <Storages/AlterCommands.h>
#include <Storages/StorageFactory.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesColumnsValidator.h>
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
    extern const int UNEXPECTED_TABLE_ENGINE;
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

    /// Generates a storage id for an inner target table.
    StorageID generateInnerTableId(const StorageID & time_series_storage_id, TargetKind inner_table_kind, const UUID & inner_uuid)
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
        TargetKind inner_table_kind, const ColumnsDescription & time_series_columns, const TimeSeriesSettings & time_series_settings)
    {
        ColumnsDescription columns;

        switch (inner_table_kind)
        {
            case TargetKind::Data:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    /// The expression for calculating the identifier of a time series can be transferred only to the "tags" inner table
                    /// (because it usually depends on columns like "metric_name" or "all_tags").
                    id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "timestamp".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Timestamp));

                /// Column "value".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Value));
                break;
            }

            case TargetKind::Tags:
            {
                /// Column "id".
                {
                    auto id_column = time_series_columns.get(TimeSeriesColumnNames::ID);
                    if (!time_series_settings.copy_id_default_to_tags_table)
                        id_column.default_desc = {};
                    columns.add(std::move(id_column));
                }

                /// Column "metric_name".
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricName));

                /// Columns corresponding to specific tags specified in the "tags_to_columns" setting.
                const Map & tags_to_columns = time_series_settings.tags_to_columns;
                for (const auto & tag_name_and_column_name : tags_to_columns)
                {
                    const auto & tuple = tag_name_and_column_name.safeGet<const Tuple &>();
                    const auto & column_name = tuple.at(1).safeGet<String>();
                    columns.add(time_series_columns.get(column_name));
                }

                /// Column "tags".
                if (time_series_settings.use_column_tags_for_other_tags)
                    columns.add(time_series_columns.get(TimeSeriesColumnNames::Tags));

                /// Column "all_tags".
                if (time_series_settings.create_ephemeral_all_tags_in_tags_table)
                {
                    ColumnDescription all_tags_column;
                    if (const auto * existing_column = time_series_columns.tryGet(TimeSeriesColumnNames::AllTags))
                        all_tags_column = *existing_column;
                    else
                        all_tags_column = ColumnDescription{TimeSeriesColumnNames::AllTags, std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>())};
                    /// Column "all_tags" is here only to calculate the identifier of a time series for the "id" column, so it can be ephemeral.
                    all_tags_column.default_desc.kind = ColumnDefaultKind::Ephemeral;
                    if (!all_tags_column.default_desc.expression)
                    {
                        all_tags_column.default_desc.ephemeral_default = true;
                        all_tags_column.default_desc.expression = makeASTFunction("defaultValueOfTypeName", std::make_shared<ASTLiteral>(all_tags_column.type->getName()));
                    }
                    columns.add(std::move(all_tags_column));
                }

                break;
            }

            case TargetKind::Metrics:
            {
                columns.add(time_series_columns.get(TimeSeriesColumnNames::MetricFamilyName));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Type));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Unit));
                columns.add(time_series_columns.get(TimeSeriesColumnNames::Help));
                break;
            }

            default:
                UNREACHABLE();
        }

        return columns;
    }

    /// Makes description of the default table engine of an inner target table.
    std::shared_ptr<ASTStorage> getDefaultEngineForInnerTable(TargetKind inner_table_kind)
    {
        auto storage = std::make_shared<ASTStorage>();
        switch (inner_table_kind)
        {
            case TargetKind::Data:
            {
                storage->set(storage->engine, makeASTFunction("MergeTree"));
                storage->engine->no_empty_args = false;

                storage->set(storage->order_by,
                             makeASTFunction("tuple",
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID),
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)));
                break;
            }
            case TargetKind::Tags:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;

                storage->set(storage->primary_key,
                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName));

                storage->set(storage->order_by,
                             makeASTFunction("tuple",
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricName),
                                             std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::ID)));
                break;
            }
            case TargetKind::Metrics:
            {
                storage->set(storage->engine, makeASTFunction("ReplacingMergeTree"));
                storage->engine->no_empty_args = false;
                storage->set(storage->order_by, std::make_shared<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
                break;
            }
            default:
                UNREACHABLE();
        }
        return storage;
    }

    /// Generates a CREATE query for creating an inner target table.
    std::shared_ptr<ASTCreateQuery> getCreateQueryForInnerTable(
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        TargetKind inner_table_kind,
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
        TargetKind target_kind,
        const ASTCreateQuery & query,
        const StorageID & time_series_storage_id,
        const ColumnsDescription & time_series_columns,
        const TimeSeriesSettings & time_series_settings,
        const ContextPtr & context,
        LoadingStrictnessLevel mode)
    {
        const auto & target = query.getTarget(target_kind);
        bool is_inner = target.table_id.empty();
        StorageID res = StorageID::createEmpty();

        if (!is_inner)
        {
            /// A target table is specified.
            res = target.table_id;
            if (mode < LoadingStrictnessLevel::ATTACH)
            {
                /// If it's not an ATTACH request then
                /// check that the specified target table has all the required columns.
                auto target_table = DatabaseCatalog::instance().getTable(target.table_id, context);
                auto target_metadata = target_table->getInMemoryMetadataPtr();
                const auto & target_columns = target_metadata->columns;
                TimeSeriesColumnsValidator validator{target.table_id};
                validator.validateTargetColumns(target_kind, target_columns, time_series_settings);
            }
        }
        else
        {
            /// An inner target table should be used.
            if (mode >= LoadingStrictnessLevel::ATTACH)
            {
                /// If it's an ATTACH request, then the inner target table must be already created.
                res = generateInnerTableId(time_series_storage_id, target_kind, target.inner_uuid);
            }
            else
            {
                /// We will make a query to create the inner target table.
                auto create_context = Context::createCopy(context);

                auto manual_create_query = getCreateQueryForInnerTable(time_series_storage_id, time_series_columns, time_series_settings,
                                                                    target_kind, target.inner_uuid, target.inner_storage);

                /// Create the inner target table.
                InterpreterCreateQuery create_interpreter(manual_create_query, create_context);
                create_interpreter.setInternal(true);
                create_interpreter.execute();

                res = DatabaseCatalog::instance().getTable({manual_create_query->getDatabase(), manual_create_query->getTable()}, context)->getStorageID();
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
