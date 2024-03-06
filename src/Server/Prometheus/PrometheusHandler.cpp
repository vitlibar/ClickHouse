#include <Server/Prometheus/PrometheusHandler.h>

#include <Server/IServer.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/authenticateUserByHTTP.h>
#include <Server/HTTP/checkHTTPHeaders.h>
#include <Server/HTTP/getSettingsOverridesFromHTTPQuery.h>
#include <Access/Credentials.h>
#include <Interpreters/Context.h>
#include <Interpreters/Session.h>
#include <IO/Protobuf/ProtobufZeroCopyInputStreamFromReadBuffer.h>
#include <IO/Protobuf/ProtobufZeroCopyOutputStreamFromWriteBuffer.h>
#include <IO/SnappyReadBuffer.h>
#include <IO/SnappyWriteBuffer.h>
#include <Storages/Prometheus/PrometheusStorage.h>
#include <Storages/Prometheus/PrometheusStorages.h>
#include <Poco/Util/LayeredConfiguration.h>

#include <prompb/remote.pb.h>


namespace DB
{

PrometheusHandler::PrometheusHandler(IServer & server_, const Configuration & config_, const AsynchronousMetrics & async_metrics_)
    : PrometheusMetricsHandler(server_, config_, async_metrics_)
    , default_settings(server_.context()->getSettingsRef())
{
}

PrometheusHandler::~PrometheusHandler() = default;


void PrometheusHandler::onException()
{
    // So that the next requests on the connection have to always start afresh in case of exceptions.
    request_credentials.reset();
}


void PrometheusHandler::handleWrapper(HTTPServerRequest & request, HTTPServerResponse & response, bool authenticate, std::function<void()> && func)
{
    SCOPE_EXIT({
        context.reset();
        session.reset();
        params.reset();
    });

    params = std::make_unique<HTMLForm>(default_settings, request);
    setShowStacktrace(params->getParsed<bool>("stacktrace", false) && server.config().getBool("enable_http_stacktrace", true));

    if (authenticate)
    {
        if (!authenticateUserAndMakeSession(request, response))
            return;
    }

    std::move(func)();
}

void PrometheusHandler::handleMetrics(HTTPServerRequest & request, HTTPServerResponse & response)
{
    handleWrapper(request, response, /* authenticate= */ false, [&] { PrometheusBaseHandler::handleMetrics(request, response); });
}

void PrometheusHandler::handleRemoteWrite(HTTPServerRequest & request, HTTPServerResponse & response)
{
    handleWrapper(request, response, /* authenticate= */ true, [&] { handleRemoteWriteImpl(request, response); });
}

void PrometheusHandler::handleRemoteRead(HTTPServerRequest & request, HTTPServerResponse & response)
{
    handleWrapper(request, response, /* authenticate= */ true, [&] { handleRemoteReadImpl(request, response); });
}

void PrometheusHandler::handleRemoteWriteImpl(HTTPServerRequest & request, HTTPServerResponse & response)
{
    LOG_INFO(log, "Handling remote write request from {}", request.get("User-Agent", ""));

    checkHTTPHeaderSetToValue(request, "Content-Type", "application/x-protobuf");
    checkHTTPHeaderSetToValue(request, "Content-Encoding", "snappy");

    auto prometheus_storage = PrometheusStorages::instance().getPrometheusStorage(config.remote_write->storage);

    ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{std::make_unique<SnappyReadBuffer>(wrapReadBufferReference(request.getStream()))};

    prometheus::WriteRequest write_request;
    if (!write_request.ParseFromZeroCopyStream(&zero_copy_input_stream))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse WriteRequest");

    if (write_request.timeseries_size())
        prometheus_storage->writeTimeSeries(write_request.timeseries(), context);
    
    if (write_request.metadata_size())
        prometheus_storage->writeMetricsMetadata(write_request.metadata(), context);

    response.setContentType("text/plain; charset=UTF-8");
    response.send();
}


void PrometheusHandler::handleRemoteReadImpl(HTTPServerRequest & request, HTTPServerResponse & response)
{
    LOG_INFO(log, "Handling remote read request from {}", request.get("User-Agent", ""));

    checkHTTPHeaderSetToValue(request, "Content-Type", "application/x-protobuf");
    checkHTTPHeaderSetToValue(request, "Content-Encoding", "snappy");

    auto prometheus_storage = PrometheusStorages::instance().getPrometheusStorage(config.remote_write->storage);

    ProtobufZeroCopyInputStreamFromReadBuffer zero_copy_input_stream{std::make_unique<SnappyReadBuffer>(wrapReadBufferReference(request.getStream()))};

    prometheus::ReadRequest read_request;
    if (!read_request.ParseFromZeroCopyStream(&zero_copy_input_stream))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse ReadRequest");

    prometheus::ReadResponse read_response;

    size_t num_queries = read_request.queries_size();
    for (size_t i = 0; i != num_queries; ++i)
    {
        const auto & query = read_request.queries(static_cast<int>(i));
        auto & new_query_result = *read_response.add_results();

        prometheus_storage->readTimeSeries(*new_query_result.mutable_timeseries(),
                                           query.start_timestamp_ms(), query.end_timestamp_ms(), query.matchers(), query.hints(),
                                           context);
    }

    LOG_INFO(&Poco::Logger::get("!!!"), "ReadResponse = {}", read_response.DebugString());

    response.setContentType("application/x-protobuf");
    response.set("Content-Encoding", "snappy");

    ProtobufZeroCopyOutputStreamFromWriteBuffer zero_copy_output_stream{std::make_unique<SnappyWriteBuffer>(getOutputStream(response))};
    read_response.SerializeToZeroCopyStream(&zero_copy_output_stream);
    zero_copy_output_stream.finalize();
}


bool PrometheusHandler::authenticateUserAndMakeSession(HTTPServerRequest & request, HTTPServerResponse & response)
{
    session = std::make_unique<Session>(server.context(), ClientInfo::Interface::PROMETHEUS, request.isSecure());

    if (!authenticateUser(request, response))
        return false;

    makeSessionContext(request);
    return true;
}


bool PrometheusHandler::authenticateUser(HTTPServerRequest & request, HTTPServerResponse & response)
{
    return authenticateUserByHTTP(request, *params, response, *session, request_credentials, server.context(), log);
}


void PrometheusHandler::makeSessionContext(HTTPServerRequest & request)
{
    auto session_context = session->makeSessionContext();

    auto can_query_param_be_setting = [&](const String & key)
    {
        static const NameSet reserved_param_names{"user", "password", "quota_key", "stacktrace"};
        return reserved_param_names.contains(key);
    };

    auto settings_changes = getSettingsOverridesFromHTTPQuery(request, *params, session_context->getSettingsRef(), can_query_param_be_setting);
    session_context->checkSettingsConstraints(settings_changes, SettingSource::QUERY);
    session_context->applySettingsChanges(settings_changes);

    context = session_context;
}

}
