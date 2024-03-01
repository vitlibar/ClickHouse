#include <Server/Prometheus/PrometheusBaseHandler.h>

#include <IO/HTTPCommon.h>
#include <IO/WriteHelpers.h>
#include <Common/Config/tryGetFromConfiguration.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/IServer.h>
#include <Poco/Util/LayeredConfiguration.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int HTTP_LENGTH_REQUIRED;
}

void PrometheusBaseHandler::Configuration::loadConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_prefix)
{
    *this = Configuration{};

    if (config_.has(config_prefix + ".remote_write"))
    {
        /// <prometheus>
        ///     <port>9363</port>
        ///     <remote_write>
        ///         <path>/write</path>
        ///         <storage>prometheus_1</storage>
        ///     </remote_write>
        /// </prometheus>
        remote_write.emplace();
        remote_write->path = config_.getString(config_prefix + ".remote_write.path", "/write");
        remote_write->storage = config_.getString(config_prefix + ".remote_write.storage");
    }

    if (config_.has(config_prefix + ".remote_read"))
    {
        /// <prometheus>
        ///     <port>9363</port>
        ///     <remote_read>
        ///         <path>/read</path>
        ///         <storage>prometheus_1</storage>
        ///     </remote_read>
        /// </prometheus>
        remote_read.emplace();
        remote_read->path = config_.getString(config_prefix + ".remote_read.path", "/read");
        remote_read->storage = config_.getString(config_prefix + ".remote_read.storage");
    }

    if (config_.has(config_prefix + ".query"))
    {
        /// <prometheus>
        ///     <port>9363</port>
        ///     <query>
        ///         <path>/query</path>
        ///         <storage>prometheus_1</storage>
        ///     </query>
        /// </prometheus>
        query.emplace();
        query->path = config_.getString(config_prefix + ".query.path", "/read");
        query->storage = config_.getString(config_prefix + ".query.storage");
    }

    if (config_.has(config_prefix + ".metrics") && !canGetBoolFromConfiguration(config_, config_prefix + ".metrics"))
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
        metrics.emplace();
        metrics->path = config_.getString(config_prefix + ".metrics.path", "/metrics");
        metrics->send_system_metrics = config_.getBool(config_prefix + ".metrics.send_system_metrics", true);
        metrics->send_system_events = config_.getBool(config_prefix + ".metrics.send_system_events", true);
        metrics->send_system_asynchronous_metrics = config_.getBool(config_prefix + ".metrics.send_system_asynchronous_metrics", true);
        metrics->send_system_errors = config_.getBool(config_prefix + ".metrics.send_system_errors", true);
    }

    if (config_.has(config_prefix + ".endpoint") || (config_.has(config_prefix) && !metrics && !remote_write && !remote_read && !query))
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
        metrics.emplace();
        metrics->path = config_.getString(config_prefix + ".metrics.endpoint", "/metrics");
        metrics->send_system_metrics = config_.getBool(config_prefix + ".metrics", true);
        metrics->send_system_events = config_.getBool(config_prefix + ".events", true);
        metrics->send_system_asynchronous_metrics = config_.getBool(config_prefix + ".asynchronous_metrics", true);
        metrics->send_system_errors = config_.getBool(config_prefix + ".errors", true);
    }

    keep_alive_timeout = config_.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
    log_name = "PrometheusHandler(" + config_prefix + ")";
}

bool PrometheusBaseHandler::Configuration::filterRequest(const HTTPServerRequest & request) const
{
    auto method = request.getMethod();
    bool is_get = (method == Poco::Net::HTTPRequest::HTTP_GET);
    bool is_post = (method == Poco::Net::HTTPRequest::HTTP_POST);

    if (metrics && (request.getURI() == metrics->path))
        return is_get;
    if (remote_write && (request.getURI() == remote_write->path))
        return is_post;
    if (remote_read && (request.getURI() == remote_read->path))
        return is_get || is_post;
    if (query && (request.getURI() == query->path))
        return is_get || is_post;

    return false;
}


PrometheusBaseHandler::PrometheusBaseHandler(IServer & server_, const Configuration & config_)
    : server(server_), config(config_), log(getLogger(config.log_name))
{
}

PrometheusBaseHandler::~PrometheusBaseHandler() = default;

void PrometheusBaseHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_)
{
    setThreadName("PromHandler");
    ThreadStatus thread_status;

    try
    {
        exception_is_written = false;
        with_stacktrace = false;
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!out);

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);
        setResponseDefaultHeaders(response, config.keep_alive_timeout);

        if (config.metrics && (request.getURI() == config.metrics->path))
        {
            handleMetrics(request, response);
        }
        else if (config.remote_write && (request.getURI() == config.remote_write->path))
        {
            handleRemoteWrite(request, response);
        }
        else if (config.remote_read && (request.getURI() == config.remote_read->path))
        {
            handleRemoteRead(request, response);
        }
        else if (config.query && (request.getURI() == config.query->path))
        {
            handleQuery(request, response);
        }
        else
        {
            handlerNotFound(request, response);
        }

        if (out)
        {
            out->finalize();
            out = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        ExecutionStatus status = ExecutionStatus::fromCurrentException("", with_stacktrace);
        trySendExceptionToClient(status.message, status.code, request, response);

        try
        {
            onException();
        }
        catch (...)
        {
            tryLogCurrentException(log, "onException");
        }
    }
}

void PrometheusBaseHandler::handlerNotFound(HTTPServerRequest & request, HTTPServerResponse & response)
{
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    writeString("There is no handler " + request.getURI() + "\n", getOutputStream(response));
}

WriteBuffer & PrometheusBaseHandler::getOutputStream(HTTPServerResponse & response)
{
    if (out)
        return *out;
    out = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, config.keep_alive_timeout, write_event);
    return *out;
}

void PrometheusBaseHandler::trySendExceptionToClient(const std::string & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
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

        if (!exception_is_written)
        {
            if (!out)
            {
                /// Nothing was sent yet.
                getOutputStream(response);
            }
            else
            {
                /// Send the error message into already used stream.
                /// Note that the error message will possibly be sent after some data.
                /// Also HTTP code 200 could have already been sent.

                /// If buffer has data, and that data wasn't sent yet, then no need to send that data
                out->position() = out->buffer().begin();
            }

            writeString(s, *out);
            writeChar('\n', *out);
            exception_is_written = true;
        }

        out->finalize();
        out = nullptr;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
        out = nullptr;
    }
}

}
