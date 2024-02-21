#pragma once

#include <Parsers/IAST_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/ColumnsDescription.h>
#include <base/types.h>
#include <prompb/remote.pb.h>
#include <mutex>


namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB
{

/// Provides access to a Prometheus storage inside ClickHouse. This storage contains three ClickHouse tables
/// `prometheus.time_series` and `prometheus.labels` and `prometheus.labels_by_name`
/// which are used to store data inserted to ClickHouse by using Prometheus remote write protocol.
class PrometheusStorage
{
public:
    PrometheusStorage(const String & prometheus_storage_id_);
    ~PrometheusStorage();

    /// Insert data received by remote write protocol to our tables.
    void addTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series, ContextPtr context);

    /// Reloads the configuration from the configuration file.
    void reloadConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Our tables.
    enum class TableKind
    {
        /// Time series. Columns: "labels_hash", "timestamp", "value". Ordered by "labels_hash" and "timestamp".
        TIME_SERIES,

        /// Labels by hash. Columns: "labels", "labels_hash". Ordered by "labels_hash". Must be a "Replacing" MergeTree table.
        LABELS,

        /// Labels by name and value. Columns: "label_name", "label_value", "labels_hash", "num_labels_in_hash".
        /// Ordered by "labels_name", "label_value", "labels_hash". Must be a "Replacing" MergeTree table.
        LABELS_BY_NAME,

        MAX,
    };

private:
    /// Prepares tables for writing or reading.
    void prepareTables(ContextPtr global_context);
    void ensureConfigLoaded(const Poco::Util::AbstractConfiguration & config);
    void loadConfig(const Poco::Util::AbstractConfiguration & config, std::unique_lock<std::mutex> * locking_mutex) TSA_NO_THREAD_SAFETY_ANALYSIS;
    void prepareCreateQueries(ContextPtr global_context);
    void ensureTablesCreated(ContextPtr global_context);

    struct BlocksToInsert;

    /// Converts data from google protobuf to blocks ready to insert into our tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series) const;

    /// Inserts blocks into our tables.
    void insertToTables(BlocksToInsert && blocks, ContextPtr context);

    const String prometheus_storage_id;
    const LoggerPtr log;
    ColumnsDescription columns_descriptions[static_cast<size_t>(TableKind::MAX)];

    struct TableConfig
    {
        String database_name;
        String table_name;
        String engine;

        bool operator==(const TableConfig & other) const { return database_name == other.database_name && table_name == other.table_name && engine == other.engine; }
        bool operator!=(const TableConfig & other) const { return !(*this == other); }
    };

    TableConfig tables_config[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool tables_config_loaded TSA_GUARDED_BY(mutex) = false;

    ASTPtr create_queries[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool create_queries_prepared TSA_GUARDED_BY(mutex) = false;

    StorageID table_ids[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool tables_created TSA_GUARDED_BY(mutex) = false;

    mutable std::mutex mutex;
};

}
