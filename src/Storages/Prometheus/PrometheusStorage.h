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
/// `prometheus.time_series` and `prometheus.labels` and `prometheus.metrics_metadata`
/// which are used to store data inserted to ClickHouse by using Prometheus remote write protocol.
class PrometheusStorage
{
public:
    PrometheusStorage(const String & prometheus_storage_id_);
    ~PrometheusStorage();

    /// Insert time series received by remote write protocol to our tables.
    void writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series, ContextPtr context);

    /// Insert metrics metadata received by remote write protocol to our tables.
    void writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata, ContextPtr context);

    /// Reloads the configuration from the configuration file.
    void reloadConfiguration(const Poco::Util::AbstractConfiguration & config);

    /// Our tables.
    enum class TableKind
    {
        /// Time series. Columns: "labels_hash" (UInt128), "timestamp" (ms, UInt64), "value" (Float64).
        /// Ordered by "labels_hash" and "timestamp".
        TIME_SERIES,

        /// Labels by metric name. Columns: "metric_name" (String), "labels" (Map(String, String)), "labels_hash" (UInt128).
        /// Ordered by "metric_name" and "labels".
        /// Note that the "labels" column is a map of labels which doesn't contain the "__name__" label
        /// (because its value is already stored in "metric_name").
        /// The "labels_hash" is a 128-bit hash calculated for both the metric name and the labels sorted lexicographically.
        /// The table engine must be a "Replacing" kinds of MergeTree table.
        LABELS,

        /// Metrics metadata. Columns: "metric_name" (String), "type" (String), "help" (String), "unit" (String).
        /// Ordered by "metric_name".
        /// The table engine must be a "Replacing" kinds of MergeTree table.
        METRICS_METADATA,

        MAX,
    };

private:
    /// Prepares tables for writing or reading.
    void prepareTables(ContextPtr global_context);
    void loadConfig(const Poco::Util::AbstractConfiguration & config);
    void loadConfigImpl(const Poco::Util::AbstractConfiguration & config, bool mutex_already_locked) TSA_NO_THREAD_SAFETY_ANALYSIS;
    void buildCreateQueries(ContextPtr global_context);
    void createTables(ContextPtr global_context);

    struct BlocksToInsert;

    /// Converts data from google protobuf to blocks ready to insert into our tables.
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series) const;
    BlocksToInsert toBlocks(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata) const;

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

    TableConfig config[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool config_loaded TSA_GUARDED_BY(mutex) = false;

    ASTPtr create_queries[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool create_queries_built TSA_GUARDED_BY(mutex) = false;

    StorageID table_ids[static_cast<size_t>(TableKind::MAX)] TSA_GUARDED_BY(mutex);
    bool tables_created TSA_GUARDED_BY(mutex) = false;

    mutable std::mutex mutex;
};

}
