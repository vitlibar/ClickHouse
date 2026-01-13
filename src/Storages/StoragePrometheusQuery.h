#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationRange.h>


namespace DB
{

/// Represents a storage for table function prometheusQuery().
class StoragePrometheusQuery : public IStorage
{
public:
    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();
        DataTypePtr timestamp_type;
        UInt32 timestamp_scale = 0;
        DataTypePtr scalar_type;

        std::shared_ptr<const PrometheusQueryTree> promql_query;
        std::optional<DateTime64> evaluation_time;
        std::optional<PrometheusQueryEvaluationRange> evaluation_range;
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context, bool over_range);

    StoragePrometheusQuery(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "PrometheusQuery"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    Configuration config;
    LoggerPtr log;
};

}
