#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/IStorage.h>


namespace DB
{

/// Represents a storage for table function timeSeriesSelector().
class StorageTimeSeriesSelector : public IStorage
{
public:
    struct Configuration
    {
        StorageID time_series_storage_id = StorageID::createEmpty();
        DataTypePtr id_type;
        DataTypePtr timestamp_data_type;
        UInt32 timestamp_scale = 0;
        DataTypePtr value_data_type;

        PrometheusQueryTree selector;
        DateTime64 min_time;
        DateTime64 max_time;
    };

    static Configuration getConfiguration(ASTs & args, const ContextPtr & context);

    StorageTimeSeriesSelector(const StorageID & table_id_, const ColumnsDescription & columns_, const Configuration & config_);

    std::string getName() const override { return "TimeSeriesSelector"; }

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
};

}
