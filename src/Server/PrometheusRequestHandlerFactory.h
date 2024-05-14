#pragma once

#include <base/types.h>
#include <memory>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{

class IServer;
class HTTPRequestHandlerFactory;
using HTTPRequestHandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;
class AsynchronousMetrics;

/// Makes a HTTP Handler factory to handle Prometheus requests, expects a config like this:
/// <prometheus>
///     <port>1234</port>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <events>true</events>
///     <errors>true</errors>
///     <asynchronous_metrics>true</asynchronous_metrics>
/// </prometheus>
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryMain(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics);

/// Makes a HTTP Handler factory to handle Prometheus requests, expects a config like this:
/// <http_port>1234</http_port>
/// <http_handlers>
///     <defaults/>
/// </http_handlers>
/// <prometheus>
///     <endpoint>/metric</endpoint>
///     <metrics>true</metrics>
///     <events>true</events>
///     <errors>true</errors>
///     <asynchronous_metrics>true</asynchronous_metrics>
/// </prometheus>
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryDefault(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics);

/// Makes a HTTP Handler factory to handle Prometheus requests, expects a config like this:
/// <http_port>1234</http_port>
/// <http_handlers>
///     <rule_my_handler>
///         <handler>
///             <type>prometheus</type>
///             <endpoint>/metrics</endpoint>
///             <send_system_metrics>true</send_system_metrics>
///             <send_system_events>true</send_system_events>
///             <send_system_errors>true</send_system_errors>
///             <asynchronous_metrics>true</asynchronous_metrics>
///         </handler>
///         <methods>HEAD,GET</methods>
///         <url>regex:.*/metrics</url>
///     </rule_my_handler>
/// </http_handlers>
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix, /// path to "http_handlers.rule_my_handler"
    const AsynchronousMetrics & asynchronous_metrics);

/// Makes a HTTP Handler factory to handle Prometheus requests for standalone clickhouse-keeper,
/// expects the same config as createPrometheusHandlerFactoryMain().
HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForKeeper(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics);

}
