#include <Server/PrometheusBaseRequestHandler.h>

#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>
#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int HTTP_LENGTH_REQUIRED;
}


PrometheusBaseRequestHandler::PrometheusBaseRequestHandler(IServer & server_, const PrometheusRequestHandlerConfig & config_)
    : server(server_), config(config_), log(getLogger("PrometheusRequestHandler"))
{
}

PrometheusBaseRequestHandler::~PrometheusBaseRequestHandler() = default;

void PrometheusBaseRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event_)
{
    setThreadName("PrometheusHndlr");
    ThreadStatus thread_status;

    try
    {
        exception_is_written = false;
        write_event = write_event_;
        http_method = request.getMethod();
        chassert(!out);

        /// Make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response, config.keep_alive_timeout);

        if (request.getURI() == config.metrics.endpoint)
            handleMetrics(request, response);
        else
            handlerNotFound(request, response);

        if (out)
        {
            out->finalize();
            out = nullptr;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);

        ExecutionStatus status = ExecutionStatus::fromCurrentException("", /* with_stacktrace= */ false);
        trySendExceptionToClient(status.message, status.code, request, response);

        out = nullptr;
    }
}

void PrometheusBaseRequestHandler::handlerNotFound(HTTPServerRequest & request, HTTPServerResponse & response)
{
    response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
    writeString("There is no handler " + request.getURI() + "\n", getOutputStream(response));
}

WriteBuffer & PrometheusBaseRequestHandler::getOutputStream(HTTPServerResponse & response)
{
    if (out)
        return *out;
    out = std::make_unique<WriteBufferFromHTTPServerResponse>(
        response, http_method == HTTPRequest::HTTP_HEAD, config.keep_alive_timeout, write_event);
    return *out;
}

void PrometheusBaseRequestHandler::trySendExceptionToClient(const String & s, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response)
{
    try
    {
        if (!exception_is_written)
        {
            response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));
            response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
        }

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

            out->finalize();
            exception_is_written = true;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Cannot send exception to client");
    }
}

}
