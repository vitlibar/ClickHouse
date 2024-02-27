#include <Server/PrometheusApiRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Server/HTTP/getSettingsOverridesFromHTTPQuery.h>
#include <Server/IServer.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

#include <Poco/Util/LayeredConfiguration.h>

#include <IO/SnappyReadBuffer.h>
#include <IO/Protobuf/ProtobufZeroCopyInputStreamFromReadBuffer.h>

#include <prompb/remote.pb.h>

#include <Prometheus/PrometheusStorage.h>
#include <Prometheus/PrometheusStorages.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <Access/Credentials.h>
#include <Interpreters/Session.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int UNKNOWN_TYPE_OF_QUERY;
    extern const int HTTP_LENGTH_REQUIRED;
}

PrometheusApiRequestHandler::PrometheusApiRequestHandler(
    IServer & server_, const Configuration & config_)
    : server(server_)
    , config(config_)
    , default_settings(server.context()->getSettingsRef())
    , prometheus_storage(PrometheusStorages::instance().getPrometheusStorage(config.prometheus_storage_id))
    , log(getLogger("PrometheusApiRequestHandler(" + config.prometheus_storage_id + ")"))
{
}

PrometheusApiRequestHandler::~PrometheusApiRequestHandler() = default;


void PrometheusApiRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & /* write_event */)
{
    setThreadName("PromApiHandler");
    ThreadStatus thread_status;

    bool with_stacktrace = false;

    try
    {
        SCOPE_EXIT({ session.reset(); });

        HTMLForm params(default_settings, request);

        if (params.getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true))
            with_stacktrace = true;

        if (!config.remote_write.empty() && (request.getURI() == config.remote_write))
        {
            handleRemoteWrite(request, params, response);
        }
        else if (!config.remote_read.empty() && (request.getURI() == config.remote_read))
        {
            handleRemoteRead(request, params, response);
        }
        else if (!config.instant_query.empty() && (request.getURI() == config.instant_query))
        {
            handleInstantQuery(request, params, response);
        }
        else
        {
            handleNotFound(request);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        // So that the next requests on the connection have to always start afresh in case of exceptions.
        request_credentials.reset();

        ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);
        trySendExceptionToClient(status.message, status.code, request, response);
    }
}


void PrometheusApiRequestHandler::handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response)
{
    if (!authenticateUserAndMakeSession(request, params, response))
        return;

    LOG_INFO(
        &Poco::Logger::get("!!!"),
        "PrometheusApiRequestHandler::handleRequest: method={}, content_type={}, context_length={}, keep_alive={}, transfer_encoding={}, version={}",
        request.getMethod(),
        request.getContentType(),
        request.getContentLength(),
        request.getKeepAlive(),
        request.getTransferEncoding(),
        request.getVersion());

    for (const auto & [k, v] : request)
        LOG_INFO(
            &Poco::Logger::get("!!!"),
            "PrometheusApiRequestHandler::handleRequest: request: key={}, value={}", k, v);

    /// method=POST, content_type=application/x-protobuf, context_length=5985

    LOG_INFO(&Poco::Logger::get("!!!"), "Reading & uncompressing data");
    ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{std::make_unique<SnappyReadBuffer>(wrapReadBufferReference(request.getStream()))};

    LOG_INFO(&Poco::Logger::get("!!!"), "Parsing WriteRequest");

    prometheus::WriteRequest write_request;
    if (!write_request.ParsePartialFromZeroCopyStream(&zero_copy_input_stream))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse WriteRequest");

    LOG_INFO(&Poco::Logger::get("!!!"), "write_request={}", write_request.DebugString());

    prometheus_storage->addTimeSeries(write_request.timeseries(), context);

    response.setContentType("text/plain; charset=UTF-8");
    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
    setResponseDefaultHeaders(response, keep_alive_timeout);

    response.send();
}


void PrometheusApiRequestHandler::handleRemoteRead(HTTPServerRequest &, const HTMLForm &, HTTPServerResponse &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Remote read not implemented");
}


void PrometheusApiRequestHandler::handleInstantQuery(HTTPServerRequest &, const HTMLForm &, HTTPServerResponse &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Instant queries not implemented");
}


void PrometheusApiRequestHandler::handleNotFound(HTTPServerRequest & request)
{
    throw Exception(ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "No handle for path {}", request.getURI());
}


bool PrometheusApiRequestHandler::authenticateUserAndMakeSession(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response)
{
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::PROMETHEUS_API, request.isSecure());

    if (!authenticateUser(request, params, response))
        return false;

    makeSessionContext(request, params);
    return true;
}


bool PrometheusApiRequestHandler::authenticateUser(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, params, response, *session, request_credentials, server.context(), log);
}


void PrometheusApiRequestHandler::makeSessionContext(HTTPServerRequest & request, const HTMLForm & params)
{
    context = session->makeSessionContext();

    auto can_query_param_be_setting = [&](const String & key)
    {
        static const NameSet reserved_param_names{"user", "password", "quota_key", "stacktrace"};
        return reserved_param_names.contains(key);
    };

    auto settings_changes = getSettingsOverridesFromHTTPQuery(request, params, context->getSettingsRef(), can_query_param_be_setting);
    context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    context->applySettingsChanges(settings_changes);
}


void PrometheusApiRequestHandler::trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));
        response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));

        /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
        /// to avoid reading part of the current request body in the next request.
        if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && response.getKeepAlive()
            && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED && !request.getStream().eof())
        {
            request.getStream().ignoreAll();
        }

        /// If nothing was sent yet and we don't even know if we must compress the response.
        WriteBufferFromHTTPServerResponse(response, request.getMethod() == HTTPRequest::HTTP_HEAD, DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT).writeln(s);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}


HTTPRequestHandlerFactoryPtr createPrometheusApiHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".storage"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "Prometheus storage is not specified for {} (no element {} in the configuration)",
                        config_prefix, config_prefix + ".storage");

    PrometheusApiRequestHandler::Configuration prometheus_config;
    prometheus_config.prometheus_storage_id = config.getString(config_prefix + ".storage");
    prometheus_config.remote_write = config.getString(config_prefix + ".remote_write", "");
    prometheus_config.remote_read = config.getString(config_prefix + ".remote_read", "");
    prometheus_config.instant_query = config.getString(config_prefix + ".instant_query", "");

    auto creator = [&server, prometheus_config]() -> std::unique_ptr<PrometheusApiRequestHandler>
    {
        return std::make_unique<PrometheusApiRequestHandler>(server, prometheus_config);
    };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusApiRequestHandler>>(std::move(creator));

    auto filter = [prometheus_config](const HTTPServerRequest & request) -> bool
    {
        auto method = request.getMethod();
        if ((method == Poco::Net::HTTPRequest::HTTP_GET) || (method == Poco::Net::HTTPRequest::HTTP_POST))
        {
            if (!prometheus_config.remote_write.empty() && (request.getURI() == prometheus_config.remote_write))
                return (method == Poco::Net::HTTPRequest::HTTP_POST);
            if (!prometheus_config.remote_read.empty() && (request.getURI() == prometheus_config.remote_read))
                return true;
            if (!prometheus_config.instant_query.empty() && (request.getURI() == prometheus_config.instant_query))
                return true;
        }
        return false;
    };

    factory->addFilter(filter);

    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusApiMainHandlerFactory(
    IServer & server, const Poco::Util::AbstractConfiguration & config, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusApiHandlerFactory(server, config, "prometheus.api"));
    return factory;
}

}
