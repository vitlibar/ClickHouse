#pragma once

#include <Common/Logger_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageTimeSeries.h>


namespace DB
{

/// Sink for inserting data into the TimeSeries table engine.
/// Transforms outer columns (time_series, metric_name, tags, metric_family, type, unit, help)
/// into blocks for the three inner target tables (Tags, Samples, Metrics).
class TimeSeriesSink : public SinkToStorage, WithContext
{
public:
    TimeSeriesSink(
        StorageTimeSeries & time_series_storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        ContextPtr context_);

    String getName() const override { return "TimeSeriesSink"; }

    void consume(Chunk & chunk) override;

private:
    StorageTimeSeries & time_series_storage;
    TimeSeriesSettingsPtr time_series_settings;
    LoggerPtr log;
};

}
