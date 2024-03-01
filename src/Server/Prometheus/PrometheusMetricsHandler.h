#pragma once

#include <Server/Prometheus/PrometheusBaseHandler.h>


namespace DB
{

class AsynchronousMetrics;

/// Handles requests for metrics ("/metrics") from Prometheus.
/// NOTE: This class is also used by standalone clickhouse-keeper, so it can't use query executing or big classes like `Context`.
class PrometheusMetricsHandler : public PrometheusBaseHandler
{
public:
    PrometheusMetricsHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusMetricsHandler() override;

protected:
    void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) override;

private:
    const AsynchronousMetrics & async_metrics;
};

}
