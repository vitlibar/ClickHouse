#pragma once

#include <Interpreters/Context_fwd.h>
#include <Storages/IStorage_fwd.h>
#include <prompb/remote.pb.h>


namespace DB
{
class StorageTimeSeries;

/// Helper class to support the prometheus remote write protocol.
class PrometheusRemoteWriteProtocol
{
public:
    PrometheusRemoteWriteProtocol(StoragePtr time_series_storage_, const ContextPtr & insert_context_);
    ~PrometheusRemoteWriteProtocol();

    /// Insert time series received by remote write protocol to our table.
    void writeTimeSeries(const google::protobuf::RepeatedPtrField<prometheus::TimeSeries> & time_series);

    /// Insert metrics metadata received by remote write protocol to our table.
    void writeMetricsMetadata(const google::protobuf::RepeatedPtrField<prometheus::MetricMetadata> & metrics_metadata);

private:
    std::shared_ptr<StorageTimeSeries> time_series_storage;
    ContextPtr context;
    Poco::LoggerPtr log;
};

}
