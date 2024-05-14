#include <Server/PrometheusRequestHandler.h>

#include <IO/HTTPCommon.h>
#include <Server/HTTP/WriteBufferFromHTTPServerResponse.h>
#include <Server/IServer.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>


namespace DB
{

PrometheusRequestHandler::PrometheusRequestHandler(
    IServer & server_, const PrometheusRequestHandlerConfig & config_, const AsynchronousMetrics & async_metrics_, const PrometheusMetricsWriterPtr & metrics_writer_)
    : server(server_)
    , config(config_)
    , async_metrics(async_metrics_)
    , metrics_writer(metrics_writer_)
    , log(getLogger("PrometheusRequestHandler"))
{
}

PrometheusRequestHandler::~PrometheusRequestHandler() = default;

void PrometheusRequestHandler::handleRequest(HTTPServerRequest & request, HTTPServerResponse & response, const ProfileEvents::Event & write_event)
{
    try
    {
        /// In order to make keep-alive works.
        if (request.getVersion() == HTTPServerRequest::HTTP_1_1)
            response.setChunkedTransferEncoding(true);

        setResponseDefaultHeaders(response, config.keep_alive_timeout);

        response.setContentType("text/plain; version=0.0.4; charset=UTF-8");

        WriteBufferFromHTTPServerResponse wb(response, request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD, config.keep_alive_timeout, write_event);
        try
        {
            if (config.metrics.send_events)
                metrics_writer->writeEvents(wb);

            if (config.metrics.send_metrics)
                metrics_writer->writeMetrics(wb);

            if (config.metrics.send_asynchronous_metrics)
                metrics_writer->writeAsynchronousMetrics(wb, async_metrics);

            if (config.metrics.send_errors)
                metrics_writer->writeErrors(wb);

            wb.finalize();
        }
        catch (...)
        {
            wb.finalize();
        }
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

}
