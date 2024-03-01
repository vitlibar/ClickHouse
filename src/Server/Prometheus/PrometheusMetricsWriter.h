#pragma once


namespace DB
{
class WriteBuffer;
class AsynchronousMetrics;

/// Write metrics in Prometheus format.
class PrometheusMetricsWriter
{
public:
    PrometheusMetricsWriter(
        bool send_metrics_,
        bool send_asynchronous_metrics_,
        bool send_events_,
        bool send_errors_,
        const AsynchronousMetrics & async_metrics_);

    void write(WriteBuffer & wb) const;

private:
    const bool send_metrics;
    const bool send_asynchronous_metrics;
    const bool send_events;
    const bool send_errors;

    const AsynchronousMetrics & async_metrics;

    static inline constexpr auto profile_events_prefix = "ClickHouseProfileEvents_";
    static inline constexpr auto current_metrics_prefix = "ClickHouseMetrics_";
    static inline constexpr auto asynchronous_metrics_prefix = "ClickHouseAsyncMetrics_";
    static inline constexpr auto error_metrics_prefix = "ClickHouseErrorMetric_";
};

}
