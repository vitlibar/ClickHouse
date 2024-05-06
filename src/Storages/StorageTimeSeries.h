#pragma once

#include <Parsers/ASTSelectQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

/// Represents a table engine to keep time series received by Prometheus protocols.
/// Examples of using this table engine:
///
/// ENGINE = TimeSeries()
/// -OR-
/// ENGINE = TimeSeries() DATA [db].table1 METRICS [db].table2 METADATA [db].table3
/// -OR-
/// ENGINE = TimeSeries() DATA ENGINE = MergeTree METRICS ENGINE = ReplacingMergeTree METADATA ENGINE = ReplacingMergeTree
/// -OR-
/// ENGINE = TimeSeries() SETTINGS id_type = 'UInt128', id_codec='ZSTD(3)' DATA ENGINE = ReplicatedMergeTree('zkpath', 'replica'), ...
///
class StorageTimeSeries final : public IStorage, WithContext
{
public:
    StorageTimeSeries(const StorageID & table_id, const ContextPtr & local_context, LoadingStrictnessLevel mode,
                      const ASTCreateQuery & query, const ColumnsDescription & columns, const String & comment);

    std::string getName() const override { return "TimeSeries"; }

    const TimeSeriesSettings & getStorageSettings() const { return storage_settings; }

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
    TimeSeriesSettings storage_settings;

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
