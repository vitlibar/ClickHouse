#include <Access/RoleList.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Parsers/ASTRoleList.h>
#include <Parsers/formatAST.h>
#include <boost/range/algorithm/sort.hpp>


namespace DB
{
RoleList::RoleList() = default;
RoleList::RoleList(const RoleList & src) = default;
RoleList & RoleList::operator =(const RoleList & src) = default;
RoleList::RoleList(RoleList && src) = default;
RoleList & RoleList::operator =(RoleList && src) = default;


RoleList::RoleList(const UUID & id)
{
    add(id);
}


RoleList::RoleList(const std::vector<UUID> & ids_)
{
    add(ids_);
}


RoleList::RoleList(const ASTRoleList & ast, const AccessControlManager & manager, const UUID & current_user_id)
{
    all = ast.all;

    if (!ast.names.empty())
    {
        ids.reserve(ast.names.size());
        for (const String & name : ast.names)
        {
            auto id = manager.getID<User>(name);
            ids.insert(id);
        }
    }

    if (ast.current_user)
        ids.insert(current_user_id);

    if (!ast.except_names.empty())
    {
        except_ids.reserve(ast.except_names.size());
        for (const String & except_name : ast.except_names)
        {
            auto except_id = manager.getID<User>(except_name);
            except_ids.insert(except_id);
        }
    }

    if (ast.except_current_user)
        except_ids.insert(current_user_id);
}

std::shared_ptr<ASTRoleList> RoleList::toAST(const AccessControlManager & manager) const
{
    auto ast = std::make_shared<ASTRoleList>();
    ast->all = all;

    if (!ids.empty())
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
        {
            auto name = manager.tryReadName(id);
            if (name)
                ast->names.emplace_back(std::move(*name));
        }
        boost::range::sort(ast->names);
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
        {
            auto except_name = manager.tryReadName(except_id);
            if (except_name)
                ast->except_names.emplace_back(std::move(*except_name));
        }
        boost::range::sort(ast->except_names);
    }

    return ast;
}


String RoleList::toString(const AccessControlManager & manager) const
{
    auto ast = toAST(manager);
    return serializeAST(*ast);
}


Strings RoleList::toStrings(const AccessControlManager & manager) const
{
    if (all)
        return {toString(manager)};

    Strings names;
    names.reserve(ids.size());
    for (const UUID & id : ids)
    {
        auto name = manager.tryReadName(id);
        if (name)
            names.emplace_back(std::move(*name));
    }
    boost::range::sort(names);
    return names;
}


bool RoleList::empty() const
{
    return ids.empty() && !all;
}


void RoleList::clear()
{
    ids.clear();
    all = false;
    except_ids.clear();
}


void RoleList::add(const UUID & id)
{
    ids.insert(id);
}


void RoleList::add(const std::vector<UUID> & ids_)
{
    for (const auto & id : ids_)
        add(id);
}


bool RoleList::match(const UUID & user_id) const
{
    if (all)
        return !except_ids.contains(user_id);
    else
        return ids.contains(user_id);
}


std::vector<UUID> RoleList::getAllMatchingUsers(const AccessControlManager & manager) const
{
    std::vector<UUID> matching_ids;
    for (const UUID & id : manager.findAll<User>())
    {
        if (match(id))
            matching_ids.push_back(id);
    }
    return matching_ids;
}


bool operator ==(const RoleList & lhs, const RoleList & rhs)
{
    return (lhs.all == rhs.all) && (lhs.ids == rhs.ids) && (lhs.except_ids == rhs.except_ids);
}

}
