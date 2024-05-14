#pragma once

#include <base/types.h>


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

    size_t keep_alive_timeout;

    void loadConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
    bool filterRequest(const HTTPServerRequest & request) const;
};

}
