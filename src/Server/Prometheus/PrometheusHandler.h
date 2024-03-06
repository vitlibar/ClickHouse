#pragma once

#include <Server/Prometheus/PrometheusMetricsHandler.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{
class HTMLForm;
class Session;
class Credentials;
struct Settings;

/// Handles requests from Prometheus including both metrics ("/metrics") and API protocols (remote write, remote read, query).
class PrometheusHandler : public PrometheusMetricsHandler
{
public:
    PrometheusHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusHandler() override;

protected:
    void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void handleRemoteRead(HTTPServerRequest & request, HTTPServerResponse & response) override;
    void onException() override;

private:
    bool authenticateUserAndMakeSession(HTTPServerRequest & request, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response);
    void makeSessionContext(HTTPServerRequest & request);

    void handleWrapper(HTTPServerRequest & request, HTTPServerResponse & response, bool authenticate, std::function<void()> && func);
    void handleRemoteWriteImpl(HTTPServerRequest & request, HTTPServerResponse & response);
    void handleRemoteReadImpl(HTTPServerRequest & request, HTTPServerResponse & response);

    const Settings & default_settings;

    std::unique_ptr<HTMLForm> params;
    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextPtr context;
};

}
