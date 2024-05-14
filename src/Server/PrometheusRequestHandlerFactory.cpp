#include <Server/PrometheusRequestHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include "Server/PrometheusMetricsWriter.h"
#include "Server/PrometheusRequestHandler.h"


namespace DB
{

namespace
{
    std::shared_ptr<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>> createPrometheusHandlingRuleHandlerFactory(
        IServer & server,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const AsynchronousMetrics & asynchronous_metrics,
        const PrometheusMetricsWriterPtr & metrics_writer)
    {
        PrometheusRequestHandlerConfig loaded_config;
        loaded_config.loadConfig(config, config_prefix);

        auto creator = [&server, loaded_config, &asynchronous_metrics, metrics_writer]() -> std::unique_ptr<PrometheusRequestHandler>
        {
            return std::make_unique<PrometheusRequestHandler>(server, loaded_config, asynchronous_metrics, metrics_writer);
        };

        auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusRequestHandler>>(std::move(creator));

        auto filter = [loaded_config](const HTTPServerRequest & request) -> bool { return loaded_config.filterRequest(request); };
        factory->addFilter(filter);

        return factory;
    }
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryMain(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlerFactoryDefault(server, config, asynchronous_metrics));
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryDefault(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<PrometheusMetricsWriter>();
    return createPrometheusHandlingRuleHandlerFactory(server, config, "prometheus", asynchronous_metrics, metrics_writer);
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<PrometheusMetricsWriter>();
    auto factory = createPrometheusHandlingRuleHandlerFactory(server, config, config_prefix + ".handler", asynchronous_metrics, metrics_writer);
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForKeeper(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto metrics_writer = std::make_shared<KeeperPrometheusMetricsWriter>();
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlingRuleHandlerFactory(server, config, "prometheus", asynchronous_metrics, metrics_writer));
    return factory;
}

}
