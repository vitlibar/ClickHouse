#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>
#include <Common/ProfileEvents.h>
#include <Interpreters/Context_fwd.h>


namespace DB
{

class IServer;
class PrometheusStorage;
class Session;
struct Settings;
class HTMLForm;
class Credentials;

class PrometheusBaseRequestHandler : public HTTPRequestHandler
{
public:
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

        String log_name;

        void loadFromConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_prefix_);
        bool filter(const HTTPServerRequest & request) const;
    };

    PrometheusBaseRequestHandler(IServer & server_, const Configuration & config_);
    ~PrometheusBaseRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_) override;

protected:
    virtual void handleMetrics(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    virtual void handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    virtual void handleRemoteRead(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    virtual void handleQuery(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);

    [[noreturn]] void handleNotFound(HTTPServerRequest & request);
    void trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);

    IServer & server;
    const Configuration config;
    const AsynchronousMetrics & async_metrics;
    const Settings & default_settings;
    const LoggerPtr log;

    ProfileEvents::Event write_event;
};


class PrometheusMetricsRequestHandler : public PrometheusBaseRequestHandler
{
public:
    PrometheusMetricsRequestHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusMetricsRequestHandler() override;

protected:
    void handleMetrics(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response) override;

private:
    const AsynchronousMetrics & async_metrics;
};


class PrometheusRequestHandler : public PrometheusMetricsRequestHandler
{
public:
    PrometheusRequestHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusRequestHandler() override;

protected:
    void handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response) override;
    void handleRemoteRead(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response) override;
    void handleQuery(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response) override;

private:
    bool authenticateUserAndMakeSession(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    void makeSessionContext(HTTPServerRequest & request, const HTMLForm & params);

    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextMutablePtr context;
};




class PrometheusApiRequestHandler : public PrometheusMetricsRequestHandler
{
public:
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

        String log_name;
    };

    PrometheusApiRequestHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_);
    ~PrometheusApiRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    [[noreturn]] void handleMetrics(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    void handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    [[noreturn]] void handleRemoteRead(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    [[noreturn]] void handleQuery(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);

    [[noreturn]] void handleNotFound(HTTPServerRequest & request);
    void trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);

    bool authenticateUserAndMakeSession(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    void makeSessionContext(HTTPServerRequest & request, const HTMLForm & params);

    IServer & server;
    const Configuration config;
    const AsynchronousMetrics & async_metrics;

    const Settings & default_settings;
    const LoggerPtr log;

    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextMutablePtr context;
};

}
