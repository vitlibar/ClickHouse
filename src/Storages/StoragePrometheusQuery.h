#if 0
#pragma once

#include <Storages/IStorage.h>


namespace DB
{
class PrometheusQueryTree;

/// Executes a query in the prometheus query language, and reads the result of it.
/// This storage is used only with table function prometheusQuery().
class StoragePrometheusQuery final : public IStorage
{
public:
    StoragePrometheusQuery(
        const StorageID & table_id_,
        const StorageID & time_series_storage_id_,

        PrometheusQueryResultType result_type,
        const ContextPtr & local_context);

    std::string getName() const override { return "PrometheusQuery"; }

    void read(
        QueryPlan & query_plan,
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

private:
    StorageID time_series_storage_id = StorageID::createEmpty();
};

}
#endif
