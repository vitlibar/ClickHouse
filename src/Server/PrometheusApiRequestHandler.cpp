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
#include <Common/Config/tryGetFromConfiguration.h>

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
    , log(getLogger("PrometheusApiRequestHandler(" + config.log_name + ")"))
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

        if (config.metrics && (request.getURI() == config.metrics->path))
        {
            handleMetrics(request, params, response);
        }
        if (config.remote_write && (request.getURI() == config.remote_write->path))
        {
            handleRemoteWrite(request, params, response);
        }
        else if (config.remote_read && (request.getURI() == config.remote_read->path))
        {
            handleRemoteRead(request, params, response);
        }
        else if (config.query && (request.getURI() == config.query->path))
        {
            handleQuery(request, params, response);
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


void PrometheusApiRequestHandler::handleMetrics(HTTPServerRequest &, const HTMLForm &, HTTPServerResponse &)
{
    LOG_INFO(log, "Handling metrics request");

    PrometheusMetricsWriter metrics_writer{async_metrics, config.metrics->send_system_events, 
        config.metrics->send_system_metrics, config.metrics->send_system_asynchronous_metrics,
        config.metrics->send_system_errors};

    /// In order to make keep-alive works.
    if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
        response.setChunkedTransferEncoding(true);

    auto keep_alive_timeout = server.config().getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
    setResponseDefaultHeaders(response, keep_alive_timeout);

    response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

    WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, keep_alive_timeout, write_event);
    try
    {
        metrics_writer.write(wb);
        wb.finalize();
    }
    catch (...)
    {
        wb.finalize();
    }
}


void PrometheusApiRequestHandler::handleRemoteWrite(HTTPServerRequest & request, const HTMLForm & params, HTTPServerResponse & response)
{
    LOG_INFO(log, "Handling remote write to {}", config.remote_write->storage);

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

    auto prometheus_storage = PrometheusStorages::instance().getPrometheusStorage(config.remote_write->storage);

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
    LOG_INFO(log, "Handling remote read to {}", config.remote_read->storage);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Remote read not implemented");
}


void PrometheusApiRequestHandler::handleQuery(HTTPServerRequest &, const HTMLForm &, HTTPServerResponse &)
{
    LOG_INFO(log, "Handling query to {}", config.query->storage);
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Queries not implemented");
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


namespace
{
    PrometheusApiRequestHandler::Configuration loadPrometheusConfig(const Poco::Util::AbstractConfiguration & config,
                                                                    const std::string & config_prefix)
    {
        PrometheusApiRequestHandler::Configuration prometheus_config;
        if (config.has(config_prefix + ".remote_write"))
        {
            /// <prometheus>
            ///     <port>9363</port>
            ///     <remote_write>
            ///         <path>/write</path>
            ///         <storage>prometheus_1</storage>
            ///     </remote_write>
            /// </prometheus>
            prometheus_config.remote_write.emplace();
            prometheus_config.remote_write->path = config.getString(config_prefix + ".remote_write.path", "/write");
            prometheus_config.remote_write->storage = config.getString(config_prefix + ".remote_write.storage");
        }

        if (config.has(config_prefix + ".remote_read"))
        {
            /// <prometheus>
            ///     <port>9363</port>
            ///     <remote_read>
            ///         <path>/read</path>
            ///         <storage>prometheus_1</storage>
            ///     </remote_read>
            /// </prometheus>
            prometheus_config.remote_read.emplace();
            prometheus_config.remote_read->path = config.getString(config_prefix + ".remote_read.path", "/read");
            prometheus_config.remote_read->storage = config.getString(config_prefix + ".remote_read.storage");
        }

        if (config.has(config_prefix + ".query"))
        {
            /// <prometheus>
            ///     <port>9363</port>
            ///     <query>
            ///         <path>/query</path>
            ///         <storage>prometheus_1</storage>
            ///     </query>
            /// </prometheus>
            prometheus_config.query.emplace();
            prometheus_config.query->path = config.getString(config_prefix + ".query.path", "/read");
            prometheus_config.query->storage = config.getString(config_prefix + ".query.storage");
        }

        LOG_INFO(&Poco::Logger::get("!!!"), "X = {}", config.has(config_prefix + ".metrics"));
        LOG_INFO(&Poco::Logger::get("!!!"), "Y = '{}'", canGetBoolFromConfiguration(config, config_prefix + ".metrics"));

        if (config.has(config_prefix + ".metrics") && !canGetBoolFromConfiguration(config, config_prefix + ".metrics"))
        {
            /// New format for metrics:
            /// <prometheus>
            ///     <port>9363</port>
            ///     <metrics>
            ///         <path>/metrics</path>
            ///         <send_system_metrics>true</send_system_metrics>
            ///         <send_system_events>true</send_system_events>
            ///         <send_system_asynchronous_metrics>true</send_system_asynchronous_metrics>
            ///         <send_system_errors>true</send_system_errors>
            ///     </metrics>
            /// </prometheus>
            prometheus_config.metrics.emplace();
            prometheus_config.metrics->path = config.getString(config_prefix + ".metrics.path", "/metrics");
            prometheus_config.metrics->send_system_metrics = config.getBool(config_prefix + ".metrics.send_system_metrics", true);
            prometheus_config.metrics->send_system_events = config.getBool(config_prefix + ".metrics.send_system_events", true);
            prometheus_config.metrics->send_system_asynchronous_metrics = config.getBool(config_prefix + ".metrics.send_system_asynchronous_metrics", true);
            prometheus_config.metrics->send_system_errors = config.getBool(config_prefix + ".metrics.send_system_errors", true);
        }

        if (config.has(config_prefix + ".endpoint") || canGetBoolFromConfiguration(config, config_prefix + ".metrics")
            || (!prometheus_config.metrics && !prometheus_config.remote_write && !prometheus_config.remote_read
                && !prometheus_config.query))
        {
            /// Old format for metrics:
            /// <prometheus>
            ///     <port>9363</port>
            ///     <endpoint>/metrics</endpoint>
            ///     <metrics>true</metrics>
            ///     <events>true</events>
            ///     <asynchronous_metrics>true</asynchronous_metrics>
            ///     <errors>true</errors>
            /// </prometheus>
            prometheus_config.metrics.emplace();
            prometheus_config.metrics->path = config.getString(config_prefix + ".metrics.endpoint", "/metrics");
            prometheus_config.metrics->send_system_metrics = config.getBool(config_prefix + ".metrics", true);
            prometheus_config.metrics->send_system_events = config.getBool(config_prefix + ".events", true);
            prometheus_config.metrics->send_system_asynchronous_metrics = config.getBool(config_prefix + ".asynchronous_metrics", true);
            prometheus_config.metrics->send_system_errors = config.getBool(config_prefix + ".errors", true);
        }

        prometheus_config.log_name = config_prefix;
        return prometheus_config;
    }
}


HTTPRequestHandlerFactoryPtr createPrometheusApiHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix)
{
    PrometheusApiRequestHandler::Configuration prometheus_config = loadPrometheusConfig(config, config_prefix);

    auto creator = [&server, prometheus_config]() -> std::unique_ptr<PrometheusApiRequestHandler>
    {
        return std::make_unique<PrometheusApiRequestHandler>(server, prometheus_config);
    };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusApiRequestHandler>>(std::move(creator));

    auto filter = [prometheus_config](const HTTPServerRequest & request) -> bool
    {
        auto method = request.getMethod();
        bool is_get = (method == Poco::Net::HTTPRequest::HTTP_GET);
        bool is_post = (method == Poco::Net::HTTPRequest::HTTP_POST);

        if (prometheus_config.metrics && (request.getURI() == prometheus_config.metrics->path))
            return is_get;
        if (prometheus_config.remote_write && (request.getURI() == prometheus_config.remote_write->path))
            return is_post;
        if (prometheus_config.remote_read && (request.getURI() == prometheus_config.remote_read->path))
            return is_get || is_post;
        if (prometheus_config.query && (request.getURI() == prometheus_config.query->path))
            return is_get || is_post;

        return false;
    };

    factory->addFilter(filter);

    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusApiMainHandlerFactory(
    IServer & server, const Poco::Util::AbstractConfiguration & config, const std::string & name)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusApiHandlerFactory(server, config, "prometheus"));
    return factory;
}

}
