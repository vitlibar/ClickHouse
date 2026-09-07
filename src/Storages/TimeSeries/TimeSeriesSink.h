#pragma once

#include <Common/Logger_fwd.h>
#include <Common/PODArray_fwd.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTViewTargets.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <QueryPipeline/BlockIO.h>

#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>


namespace DB
{
class StorageTimeSeries;
class ExpressionActions;
class IColumn;
class PushingPipelineExecutor;
struct TimeSeriesSettings;
using TimeSeriesSettingsPtr = std::shared_ptr<const TimeSeriesSettings>;

/// Sink for inserting data into the TimeSeries table engine.
/// Transforms outer columns (time_series, metric_name, tags, metric_family, type, unit, help)
/// into blocks for the target tables (Tags, Samples, RecentSamples, Metrics).
/// The sink writes the samples tables in the layout of version 2 (see TimeSeriesVersion.h): the samples of a series
/// are sorted, deduplicated and split into time buckets, each bucket makes a row with the columns
/// `id`, `samples`, `bucket`, `min_time`, `max_time`.
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

    /// Sorts tags by name, removes exact duplicates and tags with empty values,
    /// and throws if the `__name__` tag is missing or appears with conflicting values.
    static void sortTagsAndRemoveDuplicates(std::vector<std::pair<std::string_view, std::string_view>> & tags);

    /// Dispatches one row of already-sorted tags into the appropriate output columns.
    /// Every tag goes to `out_tags_names`/`out_tags_values`; tags matching a key in `columns_by_tag_name`
    /// are also copied to the corresponding column.
    static void insertSortedTagsToColumns(
        const std::vector<std::pair<std::string_view, std::string_view>> & sorted_tags,
        IColumn & out_tags_names,
        IColumn & out_tags_values,
        IColumn & out_tags_offsets,
        std::unordered_map<std::string_view, IColumn *> & columns_by_tag_name);

private:
    /// A persistent pipeline for inserting blocks into one target table.
    struct TargetPipeline
    {
        ContextMutablePtr context;
        BlockIO io;
        std::unique_ptr<PushingPipelineExecutor> executor;
        std::shared_ptr<ExpressionActions> converting_actions;

        void push(Block block) const;
        ~TargetPipeline();
    };

    void initTagsAndSamplesPipelines();
    void initMetricsPipeline();
    std::unique_ptr<TargetPipeline> createTargetPipeline(ViewTarget::Kind kind, const Block & header);

    void consumeTagsAndSamples(const Block & block);
    void consumeMetrics(const Block & block);

    /// Calculates the "id" column by applying id_generator defaults and type conversion to the tags block.
    ColumnPtr calculateId(const Block & tags_block) const;

    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;

    bool insert_tags_and_samples = false;
    bool insert_metrics = false;
    bool async_insert = false;

    /// Source header for the tags pipeline WITHOUT the `id` column.
    Block tags_header_before_id;

    /// Type of the `id` column in the tags target table.
    DataTypePtr id_type;

    /// True when the resolved id-generator references the `all_tags` identifier.
    bool id_generator_uses_all_tags = false;

    /// Precomputed ExpressionActions for calculating the "id" column from a tags block.
    std::shared_ptr<ExpressionActions> calculate_id_actions;
    std::shared_ptr<ExpressionActions> convert_id_actions;

    /// Types of the columns of the samples blocks: the samples table and the recent samples table get the same columns.
    DataTypePtr timestamp_type;
    DataTypePtr scalar_type;
    DataTypePtr samples_array_type;
    DataTypePtr bucket_type;

    /// The number of ticks of `timestamp_type` in a second, e.g. 1000 for `DateTime64(3)`.
    Int64 timestamp_scale_multiplier = 1;

    /// A pipeline writing to a samples table together with the length of its buckets.
    struct SamplesPipeline
    {
        std::unique_ptr<TargetPipeline> pipeline;
        UInt64 bucket_step_seconds = 0;
    };

    /// Builds a block for a samples table from the sorted samples of the series in the input block (see consumeTagsAndSamples).
    Block makeSamplesBlock(
        const PaddedPODArray<UInt8> & filter,
        const IColumn & id_column,
        const IColumn & ts_timestamps,
        const IColumn & ts_values,
        const PaddedPODArray<Int64> & raw_timestamps,
        const PaddedPODArray<size_t> & sorted_indices,
        const PaddedPODArray<size_t> & sorted_offsets,
        UInt64 bucket_step_seconds) const;

    std::unique_ptr<TargetPipeline> tags_pipeline;
    SamplesPipeline samples_pipeline;
    SamplesPipeline recent_samples_pipeline;
    std::unique_ptr<TargetPipeline> metrics_pipeline;
};

}
