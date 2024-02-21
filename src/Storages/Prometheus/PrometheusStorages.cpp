#include <Storages/Prometheus/PrometheusStorages.h>

#include <Storages/Prometheus/PrometheusStorage.h>
#include <boost/range/adaptor/map.hpp>


namespace DB
{

PrometheusStorages & PrometheusStorages::instance()
{
    static PrometheusStorages the_instance;
    return the_instance;
}

PrometheusStorages::~PrometheusStorages() = default;

std::shared_ptr<PrometheusStorage> PrometheusStorages::getPrometheusStorage(const String & prometheus_storage_id)
{
    std::lock_guard lock{mutex};
    auto it = storages.find(prometheus_storage_id);
    if (it == storages.end())
    {
        auto new_storage = std::make_shared<PrometheusStorage>(prometheus_storage_id);
        it = storages.emplace(prometheus_storage_id, new_storage).first;
    }
    return it->second;
}

void PrometheusStorages::reloadConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::vector<std::shared_ptr<PrometheusStorage>> current_storages;
    {
        std::lock_guard lock{mutex};
        std::ranges::copy(storages | boost::adaptors::map_values, std::back_inserter(current_storages));
    }

    for (const auto & storage : current_storages)
        storage->reloadConfiguration(config);
}

}
