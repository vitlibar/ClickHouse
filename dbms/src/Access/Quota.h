#pragma once

#include <Access/IAttributes.h>
#include <chrono>
#include <map>


namespace DB
{
/// Quota for resources consumption for specific interval. Used to limit resource usage by user.
class Quota : public IAttributes
{
public:
    enum ResourceType
    {
        QUERIES,             /// Number of queries.
        ERRORS,              /// Number of queries with exceptions.
        RESULT_ROWS,         /// Number of rows returned as result.
        RESULT_BYTES,        /// Number of bytes returned as result.
        READ_ROWS,           /// Number of rows read from tables.
        READ_BYTES,          /// Number of bytes read from tables.
        EXECUTION_TIME_USEC, /// Total amount of query execution time in microseconds.
    };
    static constexpr size_t MAX_RESOURCE_TYPE = 7;

    using ResourceAmount = size_t;
    static constexpr ResourceAmount UNLIMITED = 0;

    struct Limits
    {
        ResourceAmount limits[MAX_RESOURCE_TYPE];

        Limits();
        ResourceAmount operator[](ResourceType resource_type) const { return limits[static_cast<size_t>(resource_type)]; }
        ResourceAmount & operator[](ResourceType resource_type) { return limits[static_cast<size_t>(resource_type)]; }

        friend bool operator ==(const Limits & lhs, const Limits & rhs);
        friend bool operator !=(const Limits & lhs, const Limits & rhs) { return !(lhs == rhs); }
    };

    /// Amount of resources available to consume for each duration.
    std::map<std::chrono::seconds, Limits> limits_for_duration;

    /// Key to calculate consumption.
    /// Users with the same key share the same amount of resource.
    enum Key
    {
        NO_KEY = 0x00,            /// Resource usage is calculated in total.
        USER_NAME_AS_KEY = 0x01,  /// Resource usage is calculated for each user name separately.
        IP_ADDRESS_AS_KEY = 0x02, /// Resource usage is calculated for each IP address separately.
        ALLOW_CUSTOM_KEY = 0x04,  /// Combine with this flag to allow custom keys.
    };
    Key key = NO_KEY;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<Quota>(); }
    bool equal(const IAttributes & other) const override;
};
}
