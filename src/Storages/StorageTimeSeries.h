#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>


namespace DB
{
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

/// Represents a table engine to keep time series received by Prometheus protocols.
/// Examples of using this table engine:
///
/// CREATE TABLE ts ENGINE = TimeSeries()
/// -OR-
/// CREATE TABLE ts ENGINE = TimeSeries() DATA [db].table1 TAGS [db].table2 METRICS [db].table3
/// -OR-
/// CREATE TABLE ts ENGINE = TimeSeries() DATA ENGINE = MergeTree TAGS ENGINE = ReplacingMergeTree METRICS ENGINE = ReplacingMergeTree
/// -OR-
/// CREATE TABLE ts (id UInt128 CODEC(ZSTD(3)), instance LowCardinality(String), job String) ENGINE = TimeSeries()
///    SETTINGS id_algorithm='SipHash', tags_to_columns = {'instance': 'instance', 'job': 'job'} DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
///
class StorageTimeSeries final : public IStorage, WithContext
{
public:
    /// Adds missing columns and also reorders columns from the create query.
    /// This function is called by InterpreterCreateQuery to build a corrected version of the table's columns.
    static ColumnsDescription getActualStructure(
        const ContextPtr & local_context, const ASTCreateQuery & create_query, const ColumnsDescription & columns_from_create_query);

    StorageTimeSeries(const StorageID & table_id, const ContextPtr & local_context, LoadingStrictnessLevel mode,
                      const ASTCreateQuery & query, const ColumnsDescription & columns, const String & comment);

    ~StorageTimeSeries() override;

    std::string getName() const override { return "TimeSeries"; }

    TimeSeriesSettings getStorageSettings() const;
    TimeSeriesSettingsPtr getStorageSettingsPtr() const { return storage_settings; }

    StorageID getTargetTableId(TargetTableKind target_kind) const;
    StoragePtr getTargetTable(TargetTableKind target_kind, const ContextPtr & local_context) const;
    StoragePtr tryGetTargetTable(TargetTableKind target_kind, const ContextPtr & local_context) const;

    void startup() override;
    void shutdown(bool is_drop) override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr & query, const StorageMetadataPtr & metadata_snapshot, ContextPtr context, bool async_insert) override;

    bool optimize(
        const ASTPtr & query,
        const StorageMetadataPtr & metadata_snapshot,
        const ASTPtr & partition,
        bool final,
        bool deduplicate,
        const Names & deduplicate_by_columns,
        bool cleanup,
        ContextPtr local_context) override;

    void drop() override;
    void dropInnerTableIfAny(bool sync, ContextPtr local_context) override;

    void truncate(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, TableExclusiveLockHolder &) override;

    void renameInMemory(const StorageID & new_table_id) override;

    void checkAlterIsPossible(const AlterCommands & commands, ContextPtr local_context) const override;
    void alter(const AlterCommands & params, ContextPtr local_context, AlterLockHolder & table_lock_holder) override;

    void backupData(BackupEntriesCollector & backup_entries_collector, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;
    void restoreDataFromBackup(RestorerFromBackup & restorer, const String & data_path_in_backup, const std::optional<ASTs> & partitions) override;

    std::optional<UInt64> totalRows(const Settings & settings) const override;
    std::optional<UInt64> totalBytes(const Settings & settings) const override;
    std::optional<UInt64> totalBytesUncompressed(const Settings & settings) const override;
    Strings getDataPaths() const override;

private:
    TimeSeriesSettingsPtr storage_settings;

    struct Target
    {
        TargetTableKind kind;
        StorageID table_id = StorageID::createEmpty();
        bool has_inner_table = false;
        explicit Target(TargetTableKind kind_) : kind(kind_) {}
    };

    std::vector<Target> targets;
    bool has_inner_tables = false;
};

}
