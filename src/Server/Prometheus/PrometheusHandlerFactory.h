#pragma once

#include <Server/HTTP/HTTPRequestHandlerFactory.h>


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class IServer;
class AsynchronousMetrics;

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    AsynchronousMetrics & async_metrics);

HTTPRequestHandlerFactoryPtr createPrometheusMainHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    AsynchronousMetrics & async_metrics);

}
