#include <Access/IAttributes.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ATTRIBUTES_NOT_FOUND;
}


IAttributes::Type::Type(const char * name_, size_t namespace_idx_, const Type *  base_type_)
    : name(name_),
      namespace_idx(namespace_idx_),
      base_type(base_type_) {}


bool IAttributes::equal(const IAttributes & other) const
{
    return (name == other.name) && (getType() == other.getType());
}


bool IAttributes::Type::isDerived(const Type & base_type_) const
{
    const Type * type = this;
    while (*type != base_type_)
    {
        if (!type->base_type)
            return false;
        type = type->base_type;
    }
    return true;
}


bool IAttributes::isDerived(const Type & base_type) const
{
    return getType().isDerived(base_type);
}


void IAttributes::checkIsDerived(const Type & base_type) const
{
    if (!isDerived(base_type))
    {
        const Type & type = getType();
        throw Exception(
            String(type.name) + " " + backQuote(name) + ": expected to be of type " + base_type.name,
            ErrorCodes::ATTRIBUTES_NOT_FOUND);
    }
}
}
