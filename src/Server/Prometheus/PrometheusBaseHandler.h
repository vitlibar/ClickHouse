#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class IServer;
class HTMLForm;
class WriteBufferFromHTTPServerResponse;

/// Base class for Prometheus protocol handlers.
class PrometheusBaseHandler : public HTTPRequestHandler
{
public:
    /// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
    struct Configuration
    {
        struct PathAndStorage
        {
            String path;
            String storage;
        };

        std::optional<PathAndStorage> remote_write;
        std::optional<PathAndStorage> remote_read;
        std::optional<PathAndStorage> query;

        struct Metrics
        {
            String path;
            bool send_system_metrics = false;
            bool send_system_events = false;
            bool send_system_asynchronous_metrics = false;
            bool send_system_errors = false;
        };

        std::optional<Metrics> metrics;

        size_t keep_alive_timeout;
        String log_name;

        void loadConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
        bool filterRequest(const HTTPServerRequest & request) const;
    };

    PrometheusBaseHandler(IServer & server_, const Configuration & config_);
    ~PrometheusBaseHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_) override;

protected:
    virtual void handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response) { handlerNotFound(request, response); }
    virtual void handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response) { handlerNotFound(request, response); }
    virtual void handleRemoteRead(HTTPServerRequest & request, HTTPServerResponse & response) { handlerNotFound(request, response); }
    virtual void handleQuery(HTTPServerRequest & request, HTTPServerResponse & response) { handlerNotFound(request, response); }
    void handlerNotFound(HTTPServerRequest & request, HTTPServerResponse & response);

    WriteBuffer & getOutputStream(HTTPServerResponse & response);

    void setShowStacktrace(bool show_stacktrace) { with_stacktrace = show_stacktrace; }
    void trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);
    virtual void onException() {}

    IServer & server;
    const Configuration config;
    const LoggerPtr log;
    String http_method;

private:
    std::unique_ptr<WriteBufferFromHTTPServerResponse> out;
    ProfileEvents::Event write_event;
    bool exception_is_written = false;
    bool with_stacktrace = false;
};

}
