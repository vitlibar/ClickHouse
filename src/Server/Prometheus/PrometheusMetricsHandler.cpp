#include <Server/Prometheus/PrometheusMetricsHandler.h>

#include <Common/logger_useful.h>
#include <Server/Prometheus/PrometheusMetricsWriter.h>
#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{

PrometheusMetricsHandler::PrometheusMetricsHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_)
    : PrometheusBaseHandler(server_, config_)
    , async_metrics(async_metrics_)
{}

PrometheusMetricsHandler::~PrometheusMetricsHandler() = default;

void PrometheusMetricsHandler::handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response)
{
    LOG_INFO(log, "Handling metrics request from {}", request.get("User-Agent"));

    response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

    PrometheusMetricsWriter metrics_writer{
        config.metrics->send_system_metrics,
        config.metrics->send_system_asynchronous_metrics,
        config.metrics->send_system_events,
        config.metrics->send_system_errors,
        async_metrics};

    metrics_writer.write(getOutputStream(response));
}

}
