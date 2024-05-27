#include <Server/PrometheusRequestHandlerFactory.h>

#include <Server/HTTPHandlerFactory.h>
#include <Server/PrometheusMetricsOnlyRequestHandler.h>
#include <Server/PrometheusRequestHandler.h>


namespace DB
{

namespace
{
    template <typename RequestHandlerType = PrometheusRequestHandler>
    std::shared_ptr<HandlingRuleHTTPHandlerFactory<RequestHandlerType>> createPrometheusHandlingRuleHandlerFactory(
        IServer & server,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const AsynchronousMetrics & asynchronous_metrics)
    {
        PrometheusRequestHandlerConfig loaded_config;
        loaded_config.loadConfig(config, config_prefix);

        auto creator = [&server, loaded_config, &asynchronous_metrics]() -> std::unique_ptr<RequestHandlerType>
        {
            return std::make_unique<RequestHandlerType>(server, loaded_config, asynchronous_metrics);
        };

        auto factory = std::make_shared<HandlingRuleHTTPHandlerFactory<RequestHandlerType>>(std::move(creator));

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
    return createPrometheusHandlingRuleHandlerFactory<>(server, config, "prometheus", asynchronous_metrics);
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForRule(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto factory = createPrometheusHandlingRuleHandlerFactory<>(server, config, config_prefix + ".handler", asynchronous_metrics);
    factory->addFiltersFromConfig(config, config_prefix);
    return factory;
}

HTTPRequestHandlerFactoryPtr createPrometheusHandlerFactoryForKeeper(
    IServer & server,
    const Poco::Util::AbstractConfiguration & config,
    const String & name,
    const AsynchronousMetrics & asynchronous_metrics)
{
    auto factory = std::make_shared<HTTPRequestHandlerFactoryMain>(name);
    factory->addHandler(createPrometheusHandlingRuleHandlerFactory<KeeperPrometheusRequestHandler>(server, config, "prometheus", asynchronous_metrics));
    return factory;
}

}
