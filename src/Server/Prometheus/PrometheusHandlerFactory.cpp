#include <Server/Prometheus/PrometheusHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include <Server/Prometheus/PrometheusBaseHandler.h>

#ifdef CLICKHOUSE_KEEPER_STANDALONE_BUILD
#include <Server/Prometheus/PrometheusMetricsHandler.h>
#else
#include <Server/Prometheus/PrometheusHandler.h>
#endif



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

#ifdef CLICKHOUSE_KEEPER_STANDALONE_BUILD
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusMetricsHandler>>(
        [&server, prometheus_config, &async_metrics]()
        { return std::make_unique<PrometheusMetricsHandler>(server, prometheus_config, async_metrics); });
#else
    auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<PrometheusHandler>>(
        [&server, prometheus_config, &async_metrics]()
        { return std::make_unique<PrometheusHandler>(server, prometheus_config, async_metrics); });
#endif

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
