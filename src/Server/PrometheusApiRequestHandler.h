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

class PrometheusApiRequestHandler : public HTTPRequestHandler
{
public:
    struct Configuration
    {
        String prometheus_storage_id;
        String remote_write;
        String remote_read;
        String instant_query;
    };

    PrometheusApiRequestHandler(IServer & server_, const Configuration & config_);
    ~PrometheusApiRequestHandler() override;

    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event) override;

private:
    void handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    [[noreturn]] void handleRemoteRead(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    [[noreturn]] void handleInstantQuery(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);

    [[noreturn]] void handleNotFound(HTTPServerRequest & request);
    void trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response);

    bool authenticateUserAndMakeSession(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    bool authenticateUser(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response);
    void makeSessionContext(HTTPServerRequest & request, const HTMLForm & params);

    IServer & server;
    const Configuration config;
    const Settings & default_settings;

    std::shared_ptr<PrometheusStorage> prometheus_storage;
    LoggerPtr log;

    std::unique_ptr<Session> session;
    std::unique_ptr<Credentials> request_credentials;
    ContextMutablePtr context;
};

}
