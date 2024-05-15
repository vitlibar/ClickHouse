#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Server/PrometheusRequestHandlerConfig.h>


namespace DB
{
class IServer;
class WriteBufferFromHTTPServerResponse;

/// Base class for PrometheusRequestHandler and KeeperPrometheusRequestHandler.
class PrometheusBaseRequestHandler : public HTTPRequestHandler
{
public:
    PrometheusBaseRequestHandler(IServer & server_, const PrometheusRequestHandlerConfig & config_);
    ~PrometheusBaseRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_) override;

protected:
    /// Writes the current metrics to the response in the Prometheus format.
    virtual void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) = 0;

    /// Returns the write buffer used for the current HTTP response.
    WriteBuffer & getOutputStream(HTTPServerResponse & response);

    IServer & server;
    const PrometheusRequestHandlerConfig config;
    const LoggerPtr log;
    String http_method;

private:
    std::unique_ptr<WriteBufferFromHTTPServerResponse> out;
    ProfileEvents::Event write_event;
};

}
