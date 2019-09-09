#include <ACL/Quota2.h>
#include <ACL/ACLAttributesType.h>
#include <Common/Exception.h>
#include <limits>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENT;
}


using Operation = Quota2::Operation;


String Quota2::getResourceName(ResourceType resource_type)
{
    switch (resource_type)
    {
        case ResourceType::QUERIES: return "Queries";
        case ResourceType::ERRORS: return "Errors";
        case ResourceType::RESULT_ROWS: return "Total result rows";
        case ResourceType::RESULT_BYTES: return "Total result bytes";
        case ResourceType::READ_ROWS: return "Total rows read";
        case ResourceType::READ_BYTES: return "Total bytes read";
        case ResourceType::EXECUTION_TIME_USEC: return "Total execution time";
    }
    __builtin_unreachable();
}


Quota2::Limits::Limits()
    : limits{std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max(),
             std::numeric_limits<ResourceAmount>::max()}
{
}


const Quota2::Limits NoLimits{};


bool operator ==(const Quota2::Limits & lhs, const Quota2::Limits & rhs)
{
    return std::equal(std::begin(lhs.limits), std::end(lhs.limits), std::begin(rhs.limits));
}


ACLAttributesType Quota2::Attributes::getType() const
{
    return ACLAttributesType::QUOTA;
}


std::shared_ptr<IACLAttributes> Quota2::Attributes::clone() const
{
    auto result = std::make_shared<Attributes>();
    *result = *this;
    return result;
}


bool Quota2::Attributes::equal(const IACLAttributes & other) const
{
    if (!ACLAttributable::Attributes::equal(other))
        return false;
    const auto * o = dynamic_cast<const Attributes *>(&other);
    return o && (consumption_key == o->consumption_key) && (allow_custom_consumption_key == o->allow_custom_consumption_key)
        && (limits_for_duration == o->limits_for_duration);
}


void Quota2::setConsumptionKey(ConsumptionKey consumption_key)
{
    setConsumptionKeyOp(consumption_key).execute();
}


Operation Quota2::setConsumptionKeyOp(ConsumptionKey consumption_key)
{
    return prepareOperation([consumption_key](Attributes & attrs)
    {
        attrs.consumption_key = consumption_key;
    });
}


Quota2::ConsumptionKey Quota2::getConsumptionKey() const
{
    return getAttributesStrict()->consumption_key;
}


void Quota2::setAllowCustomConsumptionKey(bool allow)
{
    setAllowCustomConsumptionKeyOp(allow).execute();
}


Operation Quota2::setAllowCustomConsumptionKeyOp(bool allow)
{
    return prepareOperation([allow](Attributes & attrs)
    {
        attrs.allow_custom_consumption_key = allow;
    });
}


bool Quota2::isCustomConsumptionKeyAllowed() const
{
    return getAttributesStrict()->allow_custom_consumption_key;
}


void Quota2::setLimitForDuration(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit)
{
    setLimitForDurationOp(duration, resource_type, new_limit).execute();
}


Operation Quota2::setLimitForDurationOp(std::chrono::seconds duration, ResourceType resource_type, ResourceAmount new_limit)
{
    if (duration <= std::chrono::seconds::zero())
        throw Exception("The duration of a quota interval should be positive", ErrorCodes::BAD_ARGUMENT);
    if (!new_limit)
        new_limit = NoLimits[resource_type];
    return prepareOperation([duration, resource_type, new_limit](Attributes & attrs)
    {
        auto & limits = attrs.limits_for_duration[duration];
        limits[resource_type] = new_limit;
        if ((new_limit == NoLimits[resource_type]) && (limits == NoLimits))
            attrs.limits_for_duration.erase(duration);
    });
}


std::vector<std::pair<std::chrono::seconds, Quota2::ResourceAmount>> Quota2::getLimits(ResourceType resource_type) const
{
    std::vector<std::pair<std::chrono::seconds, ResourceAmount>> result;
    const auto & attrs = getAttributesStrict();
    for (const auto & [duration, limits] : attrs->limits_for_duration)
    {
        if (limits[resource_type] != NoLimits[resource_type])
            result.push_back({duration, limits[resource_type]});
    }
    return result;
}


ACLAttributesType Quota2::getType() const
{
    return ACLAttributesType::QUOTA;
}
}
