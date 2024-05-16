#include <Server/PrometheusRequestHandlerConfig.h>

#include <Core/Defines.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Poco/Util/AbstractConfiguration.h>



namespace DB
{
void PrometheusRequestHandlerConfig::loadConfig(const Poco::Util::AbstractConfiguration & config_, const String & config_prefix)
{
    *this = PrometheusRequestHandlerConfig{};

    /// We support two ways to setup the configuration for sending metrics:
    /// <prometheus>
    ///     <port>9363</port>
    ///     <endpoint>/metrics</endpoint>
    ///     <metrics>true</metrics>
    ///     <events>true</events>
    ///     <asynchronous_metrics>true</asynchronous_metrics>
    ///     <errors>true</errors>
    /// </prometheus>
    ///
    /// -and-
    ///
    /// <prometheus>
    ///     <port>9363</port>
    ///     <expose>
    ///         <endpoint>/metrics</endpoint>
    ///         <metrics>true</metrics>
    ///         <events>true</events>
    ///         <asynchronous_metrics>true</asynchronous_metrics>
    ///         <errors>true</errors>
    ///     </expose>
    /// </prometheus>
    if (config_.has(config_prefix + ".expose"))
    {
        metrics.endpoint = config_.getString(config_prefix + ".expose.endpoint", "/metrics");
        metrics.send_metrics = config_.getBool(config_prefix + ".expose.metrics", true);
        metrics.send_asynchronous_metrics = config_.getBool(config_prefix + ".expose.asynchronous_metrics", true);
        metrics.send_events = config_.getBool(config_prefix + ".expose.events", true);
        metrics.send_errors = config_.getBool(config_prefix + ".expose.errors", true);
    }
    else
    {
        metrics.endpoint = config_.getString(config_prefix + ".endpoint", "/metrics");
        metrics.send_metrics = config_.getBool(config_prefix + ".metrics", true);
        metrics.send_asynchronous_metrics = config_.getBool(config_prefix + ".asynchronous_metrics", true);
        metrics.send_events = config_.getBool(config_prefix + ".events", true);
        metrics.send_errors = config_.getBool(config_prefix + ".errors", true);
    }

    keep_alive_timeout = config_.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
}

bool PrometheusRequestHandlerConfig::filterRequest(const HTTPServerRequest & request) const
{
    const auto & path = request.getURI();
    const auto & method = request.getMethod();

    if (path == metrics.endpoint)
        return (method == Poco::Net::HTTPRequest::HTTP_GET) || (method == Poco::Net::HTTPRequest::HTTP_HEAD);

    return false;
}

}
