#include <Access/Quota.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t QUOTA_NAMESPACE_IDX;
}


const IAttributes::Type Quota::TYPE{"Quota", AccessControlNames::QUOTA_NAMESPACE_IDX, nullptr};


Quota::Limits::Limits()
{
    boost::range::fill(limits, UNLIMITED);
}


bool operator ==(const Quota::Limits & lhs, const Quota::Limits & rhs)
{
    return boost::range::equal(lhs.limits, rhs.limits);
}


bool Quota::equal(const IAttributes & other) const
{
    if (!IAttributes::equal(other))
        return false;
    const auto & other_quota = *other.cast<Quota>();
    return (limits_for_duration == other_quota.limits_for_duration) && (key == other_quota.key);
}
}

