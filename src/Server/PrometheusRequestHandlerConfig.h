#pragma once

#include <Core/QualifiedTableName.h>


namespace Poco::Util { class AbstractConfiguration; }

namespace DB
{
class HTTPServerRequest;

/// Configuration of a Prometheus protocol handler after it's parsed from a configuration file.
struct PrometheusRequestHandlerConfig
{
    struct Metrics
    {
        String endpoint;
        bool send_metrics = false;
        bool send_asynchronous_metrics = false;
        bool send_events = false;
        bool send_errors = false;
    };

    Metrics metrics;

    struct EndpointAndTableName
    {
        String endpoint;
        QualifiedTableName table_name;
    };

    std::optional<EndpointAndTableName> remote_write;

    size_t keep_alive_timeout;
    bool is_stacktrace_enabled = true;

    void loadConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    bool filterRequest(const HTTPServerRequest & request) const;
};

}
