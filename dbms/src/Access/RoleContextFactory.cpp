#include <Access/RoleContextFactory.h>


namespace DB
{
namespace
{
    std::vector<UUID> vectorDifference(const std::vector<UUID> & a, const std::vector<UUID> & b)
    {
        std::vector<UUID> result;
        result.reserve(a.size() * 2 + b.size());
        boost::range::copy(a, std::back_inserter(result));
        boost::range::copy(b, std::back_inserter(result));
        auto a_range = boost::make_iterator_range(result.begin(), result.begin() + a.size());
        auto b_range = boost::make_iterator_range(result.begin() + a.size(), result.begin() + a.size() + b.size());
        boost::range::sort(a_range);
        boost::range::sort(b_range);
        boost::range::set_difference(a_range, b_range, std::back_inserter(result));
        result.erase(result.begin(), result.begin() + a.size() + b.size());
        return result;
    }
}


RoleContextFactory::RoleContextFactory(const AccessControlManager & manager_)
    : manager(manager_), cache(600000 /* 10 minutes */) {}


RoleContextPtr RoleContextFactory::createContext(const std::vector<UUID> & roles, const std::vector<UUID> & roles_with_admin_option)
{
    if (roles.size() == 1 && roles_with_admin_option.empty())
        return createContextImpl(roles[0], false);

    if (roles.size() == 1 && roles_with_admin_option == roles)
        return createContextImpl(roles[0], true);

    std::vector<UUID> roles_without_admin_option = difference(roles, roles_with_admin_option);
    std::vector<RoleContextPtr> children;
    children.reserve(roles.size());
    for (const auto & role : roles_without_admin_option)
        children.push_back(createContextImpl(role, false));
    for (const auto & role : roles_with_admin_option)
        children.push_back(createContextImpl(role, true));
    return std::make_shared<RoleContext>(std::move(children));
}


RoleContextPtr RoleContextFactory::createContextImpl(const UUID & id, bool with_admin_option)
{
    std::lock_guard lock{mutex};
    auto key = std::make_pair(id, with_admin_option);
    auto x = cache.get(key);
    if (x)
        return *x;
    auto res = std::make_shared<RoleContext>(manager, id, with_admin_option);
    cache.add(key, res);
    return res;
}

}
