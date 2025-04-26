#if 0
#include <Storages/TimeSeries/PrometheusQueryPlanBuilder.h>

#include <Interpreters/DatabaseCatalog.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/ParsedPrometheusQuery.h>


namespace DB
{

PrometheusQueryPlanBuilder::PrometheusQueryPlanBuilder(
    std::shared_ptr<ParsedPrometheusQuery> promql_query_, const StorageID & time_series_storage_id_)
    : promql_query(promql_query_)
    , time_series_storage_id(time_series_storage_id_)
{
}


PrometheusQueryPlanBuilder::~PrometheusQueryPlanBuilder() = default;


void PrometheusQueryPlanBuilder::buildQueryPlan(
    QueryPlan & query_plan, const ContextPtr & context, size_t max_block_size, size_t num_streams)
{
    /// TODO: Extract these from settings.
    auto now = std::chrono::system_clock::now();
    DecimalField<DateTime64> evaluation_time{std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count(), 3};
    UInt64 lookback_delta_ms = 5 * 60 * 1000;

    parsed_promql_query.findMatchersAndTimeRanges(evaluation_time, lookback_delta_ms);

    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto tags_storage = time_series_storage->getTargetTable(ViewTarget::Tags, context);

    Names column_names{"id", "metric_name", "tags"};

    auto storage_snapshot = tags_storage->getStorageSnapshot(tags_storage->getInMemoryMetadataPtr(), context);
    SelectQueryInfo query_info;

    tags_storage->read(
        query_plan, column_names, storage_snapshot, query_info, context, QueryProcessingStage::Enum::Complete, max_block_size, num_streams);
}

}
#endif
