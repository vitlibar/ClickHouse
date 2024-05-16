#pragma once

#include <Server/PrometheusMetricsOnlyRequestHandler.h>


namespace DB
{

/// Handles requests from Prometheus including both metrics ("/metrics") and API protocols (remote write, remote read, query).
class PrometheusRequestHandler : public PrometheusMetricsOnlyRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const PrometheusRequestHandlerConfig & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusRequestHandler() override;

protected:
    void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void onException() override;

private:
    bool authenticateUserAndMakeSession(HTTPServerRequest & request, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response);
    void makeSessionContext(HTTPServerRequest & request);

    void wrapHandler(HTTPServerRequest & request, HTTPServerResponse & response, bool authenticate, std::function<void()> && func);
    void handleRemoteWriteImpl(HTTPServerRequest & request, HTTPServerResponse & response);

    const Settings & default_settings;
    std::unique_ptr<HTMLForm> params;
    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextPtr context;
};

}
