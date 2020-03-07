#include <Access/GeneralizedRoleSet.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Parsers/ASTGeneralizedRoleSet.h>
#include <Parsers/formatAST.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm/sort.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


GeneralizedRoleSet::GeneralizedRoleSet() = default;
GeneralizedRoleSet::GeneralizedRoleSet(const GeneralizedRoleSet & src) = default;
GeneralizedRoleSet & GeneralizedRoleSet::operator =(const GeneralizedRoleSet & src) = default;
GeneralizedRoleSet::GeneralizedRoleSet(GeneralizedRoleSet && src) = default;
GeneralizedRoleSet & GeneralizedRoleSet::operator =(GeneralizedRoleSet && src) = default;


GeneralizedRoleSet::GeneralizedRoleSet(AllTag)
{
    all = true;
}

GeneralizedRoleSet::GeneralizedRoleSet(const UUID & id)
{
    add(id);
}


GeneralizedRoleSet::GeneralizedRoleSet(const std::vector<UUID> & ids_)
{
    add(ids_);
}


GeneralizedRoleSet::GeneralizedRoleSet(const boost::container::flat_set<UUID> & ids_)
{
    add(ids_);
}


GeneralizedRoleSet::GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast)
{
    init(ast, nullptr, nullptr);
}

GeneralizedRoleSet::GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const UUID & current_user_id)
{
    init(ast, nullptr, &current_user_id);
}

GeneralizedRoleSet::GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const AccessControlManager & manager)
{
    init(ast, &manager, nullptr);
}

GeneralizedRoleSet::GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const AccessControlManager & manager, const UUID & current_user_id)
{
    init(ast, &manager, &current_user_id);
}

void GeneralizedRoleSet::init(const ASTGeneralizedRoleSet & ast, const AccessControlManager * manager, const UUID * current_user_id)
{
    all = ast.all;

    auto name_to_id = [id_mode{ast.id_mode}, manager](const String & name) -> UUID
    {
        if (id_mode)
            return parse<UUID>(name);
        assert(manager);
        auto id = manager->find<User>(name);
        if (id)
            return *id;
        return manager->getID<Role>(name);
    };

    if (!ast.names.empty() && !all)
    {
        ids.reserve(ast.names.size());
        for (const String & name : ast.names)
            ids.insert(name_to_id(name));
    }

    if (ast.current_user && !all)
    {
        assert(current_user_id);
        ids.insert(*current_user_id);
    }

    if (!ast.except_names.empty())
    {
        except_ids.reserve(ast.except_names.size());
        for (const String & except_name : ast.except_names)
            except_ids.insert(name_to_id(except_name));
    }

    if (ast.except_current_user)
    {
        assert(current_user_id);
        except_ids.insert(*current_user_id);
    }

    for (const UUID & except_id : except_ids)
        ids.erase(except_id);
}


std::shared_ptr<ASTGeneralizedRoleSet> GeneralizedRoleSet::toAST() const
{
    auto ast = std::make_shared<ASTGeneralizedRoleSet>();
    ast->id_mode = true;
    ast->all = all;

    if (!ids.empty())
    {
        ast->names.reserve(ids.size());
        for (const UUID & id : ids)
            ast->names.emplace_back(::DB::toString(id));
    }

    if (!except_ids.empty())
    {
        ast->except_names.reserve(except_ids.size());
        for (const UUID & except_id : except_ids)
            ast->except_names.emplace_back(::DB::toString(except_id));
    }

    return ast;
}


String GeneralizedRoleSet::toString() const
{
    auto ast = toAST();
    return serializeAST(*ast);
}


Strings GeneralizedRoleSet::toStrings() const
{
    if (all || !except_ids.empty())
        return {toString()};

    Strings names;
    names.reserve(ids.size());
    for (const UUID & id : ids)
        names.emplace_back(::DB::toString(id));
    return names;
}


std::shared_ptr<ASTGeneralizedRoleSet> GeneralizedRoleSet::toASTWithNames(const AccessControlManager & manager) const
{
    auto ast = std::make_shared<ASTGeneralizedRoleSet>();
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


String GeneralizedRoleSet::toStringWithNames(const AccessControlManager & manager) const
{
    auto ast = toASTWithNames(manager);
    return serializeAST(*ast);
}


Strings GeneralizedRoleSet::toStringsWithNames(const AccessControlManager & manager) const
{
    if (all || !except_ids.empty())
        return {toStringWithNames(manager)};

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


bool GeneralizedRoleSet::empty() const
{
    return ids.empty() && !all;
}


void GeneralizedRoleSet::clear()
{
    ids.clear();
    all = false;
    except_ids.clear();
}


void GeneralizedRoleSet::add(const UUID & id)
{
    ids.insert(id);
}


void GeneralizedRoleSet::add(const std::vector<UUID> & ids_)
{
    for (const auto & id : ids_)
        add(id);
}


void GeneralizedRoleSet::add(const boost::container::flat_set<UUID> & ids_)
{
    for (const auto & id : ids_)
        add(id);
}


bool GeneralizedRoleSet::match(const UUID & id) const
{
    return (all || ids.contains(id)) && !except_ids.contains(id);
}


bool GeneralizedRoleSet::match(const UUID & user_id, const std::vector<UUID> & enabled_roles) const
{
    if (!all && !ids.contains(user_id))
    {
        bool found_enabled_role = std::any_of(
            enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return ids.contains(enabled_role); });
        if (!found_enabled_role)
            return false;
    }

    if (except_ids.contains(user_id))
        return false;

    bool in_except_list = std::any_of(
        enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return except_ids.contains(enabled_role); });
    if (in_except_list)
        return false;

    return true;
}


bool GeneralizedRoleSet::match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const
{
    if (!all && !ids.contains(user_id))
    {
        bool found_enabled_role = std::any_of(
            enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return ids.contains(enabled_role); });
        if (!found_enabled_role)
            return false;
    }

    if (except_ids.contains(user_id))
        return false;

    bool in_except_list = std::any_of(
        enabled_roles.begin(), enabled_roles.end(), [this](const UUID & enabled_role) { return except_ids.contains(enabled_role); });
    if (in_except_list)
        return false;

    return true;
}


std::vector<UUID> GeneralizedRoleSet::getMatchingIDs() const
{
    if (all)
        throw Exception("getAllMatchingIDs() can't get ALL ids", ErrorCodes::LOGICAL_ERROR);
    std::vector<UUID> res;
    boost::range::set_difference(ids, except_ids, std::back_inserter(res));
    return res;
}


std::vector<UUID> GeneralizedRoleSet::getMatchingUsers(const AccessControlManager & manager) const
{
    if (!all)
        return getMatchingIDs();

    std::vector<UUID> res;
    for (const UUID & id : manager.findAll<User>())
    {
        if (match(id))
            res.push_back(id);
    }
    return res;
}


std::vector<UUID> GeneralizedRoleSet::getMatchingRoles(const AccessControlManager & manager) const
{
    if (!all)
        return getMatchingIDs();

    std::vector<UUID> res;
    for (const UUID & id : manager.findAll<Role>())
    {
        if (match(id))
            res.push_back(id);
    }
    return res;
}


std::vector<UUID> GeneralizedRoleSet::getMatchingUsersAndRoles(const AccessControlManager & manager) const
{
    if (!all)
        return getMatchingIDs();

    std::vector<UUID> vec = getMatchingUsers(manager);
    boost::range::push_back(vec, getMatchingRoles(manager));
    return vec;
}


bool operator ==(const GeneralizedRoleSet & lhs, const GeneralizedRoleSet & rhs)
{
    return (lhs.all == rhs.all) && (lhs.ids == rhs.ids) && (lhs.except_ids == rhs.except_ids);
}

}
