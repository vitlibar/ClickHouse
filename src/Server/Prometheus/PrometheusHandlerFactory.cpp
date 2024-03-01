#include <Server/Prometheus/PrometheusHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include <Server/Prometheus/PrometheusBaseHandler.h>
#include <Server/Prometheus/PrometheusMetricsHandler.h>


namespace DB
{

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactory(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    AsynchronousMetrics & async_metrics)
{
    PrometheusBaseHandler::Configuration prometheus_config;
    prometheus_config.loadConfig(config, config_prefix);

    auto creator = [&server, prometheus_config, &async_metrics]()
    {
        return std::make_unique<PrometheusMetricsHandler>(server, prometheus_config, async_metrics);
    };

    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusMetricsHandler>>(std::move(creator));

    auto filter = [prometheus_config](const HTTPServerRequest & request) -> bool { return prometheus_config.filterRequest(request); };
    factory->addFilter(filter);

    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusMainHandlerFactory(
    IServer & server, const Poco::Util::AbstractConfiguration & config, const std::string & name,
    AsynchronousMetrics & async_metrics)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlerFactory(server, config, "prometheus", async_metrics));
    return factory;
}

}
