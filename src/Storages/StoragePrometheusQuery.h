#pragma once

#include <Storages/IStorage.h>


namespace DB
{
class ParsedPrometheusQuery;

/// Executes a query in the prometheus query language, and reads the result of it.
/// This storage is used only with table function prometheusQuery().
class StoragePrometheusQuery final : public IStorage
{
public:
    StoragePrometheusQuery(
        const StorageID & table_id_,
        std::shared_ptr<const ParsedPrometheusQuery> parsed_promql_query_,
        const StorageID & time_series_storage_id_,
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
    std::shared_ptr<const ParsedPrometheusQuery> parsed_promql_query;
    StorageID time_series_storage_id = StorageID::createEmpty();
};

}
