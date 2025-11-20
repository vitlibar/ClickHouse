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
    class Configuration
    {
    public:
        PrometheusQueryTree promql_query;
        StorageID time_series_storage_id = StorageID::createEmpty();
        DataTypePtr timestamp_type;
        DataTypePtr scalar_type;

        /// Either `evaluation_time` or `evaluation_range` should be set.
        /// Evaluate a prometheus query at a specified evaluation time.
        Field evaluation_time;
        /// Evaluate a prometheus query over a range of time.
        PrometheusQueryEvaluationRange evaluation_range;
    };

    static Configuration getConfiguration(ASTs & args, ContextPtr context, bool is_query_range);

    StoragePrometheusQuery(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & configuration_);

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
    Configuration configuration;
    LoggerPtr log;
};

}
