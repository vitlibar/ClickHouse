#include <Server/PrometheusRequestHandlerConfig.h>

#include <Core/Defines.h>
#include <Server/HTTP/HTTPServerRequest.h>
#include <Poco/Util/AbstractConfiguration.h>



namespace DB
{

namespace
{
    QualifiedTableName loadTableNameFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        QualifiedTableName res;
        res.table = config.getString(config_prefix + ".table", "prometheus");
        res.database = config.getString(config_prefix + ".database", "");
        if (res.database.empty())
            res = QualifiedTableName::parseFromString(res.table);
        if (res.database.empty())
            res.database = "default";
        return res;
    }
}

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

    if (config_.has(config_prefix + ".remote_write"))
    {
        remote_write.emplace();
        remote_write->endpoint = config_.getString(config_prefix + ".remote_write.endpoint", "/write");
        remote_write->table_name = loadTableNameFromConfig(config_, config_prefix + ".remote_write");
    }

    if (config_.has(config_prefix + ".remote_read"))
    {
        remote_read.emplace();
        remote_read->endpoint = config_.getString(config_prefix + ".remote_read.endpoint", "/read");
        remote_read->table_name = loadTableNameFromConfig(config_, config_prefix + ".remote_read");
    }

    is_stacktrace_enabled = config_.getBool(config_prefix + ".enable_stacktrace", true);
    keep_alive_timeout = config_.getUInt("keep_alive_timeout", DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT);
}

bool PrometheusRequestHandlerConfig::filterRequest(const HTTPServerRequest & request) const
{
    const auto & path = request.getURI();
    const auto & method = request.getMethod();

    if (path == metrics.endpoint)
        return (method == Poco::Net::HTTPRequest::HTTP_GET) || (method == Poco::Net::HTTPRequest::HTTP_HEAD);

    if (remote_write && (path == remote_write->endpoint))
        return (method == Poco::Net::HTTPRequest::HTTP_POST);

    if (remote_read && (path == remote_read->endpoint))
        return (method == Poco::Net::HTTPRequest::HTTP_GET) || (method == Poco::Net::HTTPRequest::HTTP_POST);

    return false;
}

}
