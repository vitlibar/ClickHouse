#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace Poco::Util
{
    class AbstractConfiguration;
};

namespace DB
{
class PrometheusStorage;

/// Provides access to Prometheus storages inside ClickHouse.
/// Those storages are declared in the configuration file and identified by `prometheus_storage_id`:
/// <prometheus>
///     <experimental>
///         <storages>
///             <prometheus_1>
///                 <time_series>
///                     <database>prometheus</name>
///                     <table>time_series</table>
///                 </time_series>
///                 <labels>
///                     <database>prometheus</database>
///                     <table>labels</table>
///                 </labels>
///             </prometheus_1>
///         </storages>
///     </experimental>
/// </prometheus>
class PrometheusStorages
{
public:
    static PrometheusStorages & instance();

    /// Returns a Prometheus storage identified by a passed id.
    std::shared_ptr<PrometheusStorage> getPrometheusStorage(const String & prometheus_storage_id);

    /// Reread the configuration from the configuration file.
    void reloadConfiguration(const Poco::Util::AbstractConfiguration & config);

    ~PrometheusStorages();

private:
    std::unordered_map<String, std::shared_ptr<PrometheusStorage>> storages TSA_GUARDED_BY(mutex);
    std::mutex mutex;
};

}
