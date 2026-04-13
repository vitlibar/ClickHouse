#pragma once

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageTimeSeries.h>

namespace DB
{

struct BlockIO;
class ExpressionActions;
class PushingPipelineExecutor;

/// Sink for inserting data into the TimeSeries table engine.
/// Transforms outer columns (time_series, metric_name, tags, metric_family, type, unit, help)
/// into blocks for the three inner target tables (Tags, Samples, Metrics).
class TimeSeriesSink : public SinkToStorage, WithContext
{
public:
    /// `insert_columns` contains column names from the INSERT query,
    /// empty `insert_columns` means all columns from `header_`.
    TimeSeriesSink(
        StorageTimeSeries & time_series_storage_,
        const Block & header_,
        const Names & insert_columns_,
        ContextPtr context_,
        bool async_insert_);

    String getName() const override { return "TimeSeriesSink"; }

    void consume(Chunk & chunk) override;
    void onFinish() override;

private:
    /// A persistent pipeline for inserting blocks into one target table.
    struct TargetPipeline
    {
        ContextMutablePtr context;
        BlockIO io;
        std::unique_ptr<PushingPipelineExecutor> executor;
        std::shared_ptr<ExpressionActions> converting_actions;

        ~TargetPipeline();
    };

    void initTagsAndSamplesPipelines();
    void initMetricsPipeline();
    std::unique_ptr<TargetPipeline> createTargetPipeline(ViewTarget::Kind kind, const Block & header);

    void consumeTagsAndSamples(const Block & block);
    void consumeMetrics(const Block & block);

    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;

    bool insert_tags_and_samples = false;
    bool insert_metrics = false;
    bool async_insert = false;

    /// Precomputed ExpressionActions for calculating the "id" column from a tags block.
    std::shared_ptr<ExpressionActions> id_defaults_actions;
    std::shared_ptr<ExpressionActions> id_convert_actions;

    std::unique_ptr<TargetPipeline> tags_pipeline;
    std::unique_ptr<TargetPipeline> samples_pipeline;
    std::unique_ptr<TargetPipeline> metrics_pipeline;
};

}
