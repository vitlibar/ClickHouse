#include <Server/HTTP/sendExceptionToHTTPClient.h>

#include <Server/HTTP/HTTPServerRequest.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/HTTP/exceptionCodeToHTTPStatus.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int HTTP_LENGTH_REQUIRED;
    extern const int REQUIRED_PASSWORD;
}


void sendExceptionToHTTPClient(const String & exception_message, int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, WriteBufferFromHTTPServerResponse * out)
{
    setHTTPResponseStatusAndHeadersForException(exception_code, request, response, out);

    if (!out)
    {
        /// If nothing was sent yet.
        WriteBufferFromHTTPServerResponse out_for_message{response, request.getMethod() == HTTPRequest::HTTP_HEAD, DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT};

        out_for_message.writeln(exception_message);
        out_for_message.finalize();
    }
    else
    {
        /// If buffer has data, and that data wasn't sent yet, then no need to send that data
        bool data_sent = (out->count() != out->offset());

        if (!data_sent)
            out->position() = out->buffer().begin();

        out->writeln(exception_message);
        out->finalize();
    }
}


void setHTTPResponseStatusAndHeadersForException(int exception_code, HTTPServerRequest & request, HTTPServerResponse & response, WriteBufferFromHTTPServerResponse * out)
{
    if (out)
        out->setExceptionCode(exception_code);
    else
        response.set("X-ClickHouse-Exception-Code", toString<int>(exception_code));

    /// If HTTP method is POST and Keep-Alive is turned on, we should read the whole request body
    /// to avoid reading part of the current request body in the next request.
    if (request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST && response.getKeepAlive()
        && exception_code != ErrorCodes::HTTP_LENGTH_REQUIRED && !request.getStream().eof())
    {
        request.getStream().ignoreAll();
    }

    if (exception_code == ErrorCodes::REQUIRED_PASSWORD)
        response.requireAuthentication("ClickHouse server HTTP API");
    else
        response.setStatusAndReason(exceptionCodeToHTTPStatus(exception_code));
}

}
