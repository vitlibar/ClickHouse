#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{
class IServer;
class AsynchronousMetrics;
class PrometheusMetricsWriter;
using PrometheusMetricsWriterPtr = std::shared_ptr<const PrometheusMetricsWriter>;

class PrometheusRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfig & config_, const AsynchronousMetrics & async_metrics_, const PrometheusMetricsWriterPtr & metrics_writer_);
    ~PrometheusRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

protected:
    IServer & server;
    const PrometheusRequestHandlerConfig config;
    const AsynchronousMetrics & async_metrics;
    const PrometheusMetricsWriterPtr metrics_writer;
    const LoggerPtr log;
};

}
