#include <Interpreters/Access/InterpreterGrantQuery.h>
#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/Role.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/User.h>
#include <Interpreters/Context.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/executeDDLQueryOnCluster.h>
#include <boost/range/algorithm/copy.hpp>
#include <boost/range/algorithm/set_algorithm.hpp>
#include <boost/range/algorithm_ext/erase.hpp>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// Collects recipients of the GRANT or REVOKE command, i.e. users and roles which are going to be changed.
    /// `current_user` may be null only if it's executed to ATTACH queries at startup.
    std::unordered_map<UUID, AccessEntityPtr>
    collectGrantees(const ASTGrantQuery & query, const std::shared_ptr<const ContextAccess> & current_user)
    {
        if (!query.grantees)
            return {};
        const AccessControl * access_control = current_user ? current_user->getAccessControl() : nullptr;
        std::unordered_map<UUID, AccessEntityPtr> res;
        if (access_control)
        {
            auto ids = RolesOrUsersSet{*query.grantees, *access_control, current_user->getUserID()}.getMatchingIDs(*access_control);
            for (const auto & id : ids)
                res.emplace(id, access_control->tryRead(id));
        }
        else
        {
            auto ids = RolesOrUsersSet{*query.grantees}.getMatchingIDs();
            for (const auto & id : ids)
                res.emplace(id, nullptr);
        }
        return res;
    }

    /// Checks that all recipients are allowed for the current user.
    void checkGranteesAreAllowed(const std::unordered_map<UUID, AccessEntityPtr> & grantees, const std::shared_ptr<const ContextAccess> & current_user)
    {
        if (!current_user)
            return;
        for (const auto & [id, grantee] : grantees)
        {
            if (grantee)
                current_user->checkGranteeIsAllowed(id, *grantee);
        }
    }

    struct RightsToGrantAndRevoke
    {
        AccessRightsElements rights_to_grant;
        AccessRightsElements rights_to_revoke;
    };

    /// Extracts access rights elements which are going to be granted or revoked from a query.
    /// `current_user` may be null only if it's executed in the global context.
    RightsToGrantAndRevoke
    collectRightsToGrantAndRevoke(const ASTGrantQuery & query, const std::unordered_map<UUID, AccessEntityPtr> & grantees)
    {
        AccessRightsElements rights_to_grant, rights_to_revoke;

        if (query.is_revoke)
        {
            /// REVOKE
            rights_to_revoke = query.access_rights_elements;
        }
        else if (query.replace_access)
        {
            /// GRANT WITH REPLACE OPTION
            rights_to_grant = query.access_rights_elements;
            rights_to_revoke.emplace_back(AccessType::ALL);
        }
        else
        {
            /// GRANT
            rights_to_grant = query.access_rights_elements;
        }

        /// It's impossible to revoke more than a user have.
        if (!rights_to_revoke.empty())
        {
            AccessRights already_granted_to_grantees;
            for (const auto & [id, grantee] : grantees)
            {
                if (auto role = typeid_cast<RolePtr>(grantee))
                    already_granted_to_grantees.makeUnion(role->access);
                else if (auto user = typeid_cast<UserPtr>(grantee))
                    already_granted_to_grantees.makeUnion(user->access);
            }

            AccessRights intersection;
            std::for_each(rights_to_revoke.begin(), rights_to_revoke.end(), [&](AccessRightsElement & element) { element.is_partial_revoke = false; });
            intersection.grant(rights_to_revoke);
            intersection.makeIntersection(already_granted_to_grantees);

            /// Build more accurate list of elements to revoke, now we use an intesection of the initial list of elements to revoke
            /// and all the granted access rights to these grantees.
            bool grant_option = !rights_to_revoke.empty() && rights_to_revoke[0].grant_option;
            rights_to_revoke.clear();
            for (auto & element : intersection.getElements())
            {
                if (!element.is_partial_revoke && (element.grant_option || !grant_option))
                    rights_to_revoke.emplace_back(std::move(element));
            }
        }

        AccessRightsElements res;
        res.reserve(rights_to_grant.size() + rights_to_revoke.size());
        res = std::move(rights_to_grant);
        res.insert(res.end(), rights_to_revoke.begin(), rights_to_revoke.end());
        return res;
    }

    struct RolesToGrantAndRevoke
    {
        std::unordered_map<UUID, AccessEntityPtr> roles_to_grant;
        std::unordered_map<UUID, AccessEntityPtr> roles_to_revoke;
    };

    /// Extracts roles which are going to be granted or revoked from a query.
    RolesToGrantAndRevoke collectRolesToGrantAndRevoke(const ASTGrantQuery & query, const AccessControl & access_control, const std::unordered_map<UUID, AccessEntityPtr> & grantees)
    {

    }

    void checkGrantOption(const AccessRightsElements & elements, const std::shared_ptr<const ContextAccess> & current_user)
    {

    }

    void checkAdminOption(const RolesToGrantOrRevoke & roles, const ContextAccess & current_user_access)
    {

    }

#if 0
    AccessRightsElements filterAccessRightsByGrantOption(const AccessRightsElements & elements, const ContextAccess & current_user_access)
    {

    }

    RolesToGrantOrRevoke filterRolesByAdminOption(const RolesToGrantOrRevoke & roles, const ContextAccess & current_user_access)
    {

    }
#endif

    void collectRolesToGrantOrRevoke(
        const AccessControl & access_control,
        const ASTGrantQuery & query,
        std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke)
    {
        roles_to_grant.clear();
        roles_to_revoke.clear();

        RolesOrUsersSet roles_to_grant_or_revoke;
        if (query.roles)
            roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles, access_control};

        if (query.is_revoke)
        {
            /// REVOKE
            roles_to_revoke = std::move(roles_to_grant_or_revoke);
        }
        else if (query.replace_granted_roles)
        {
            /// GRANT WITH REPLACE OPTION
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs(access_control);
            roles_to_revoke = RolesOrUsersSet::AllTag{};
        }
        else
        {
            /// GRANT
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs(access_control);
        }
    }

    /// Extracts roles which are going to be granted or revoked from a query.
    void collectRolesToGrantOrRevoke(
        const ASTGrantQuery & query,
        std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke)
    {
        roles_to_grant.clear();
        roles_to_revoke.clear();

        RolesOrUsersSet roles_to_grant_or_revoke;
        if (query.roles)
            roles_to_grant_or_revoke = RolesOrUsersSet{*query.roles};

        if (query.is_revoke)
        {
            /// REVOKE
            roles_to_revoke = std::move(roles_to_grant_or_revoke);
        }
        else if (query.replace_granted_roles)
        {
            /// GRANT WITH REPLACE OPTION
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs();
            roles_to_revoke = RolesOrUsersSet::AllTag{};
        }
        else
        {
            /// GRANT
            roles_to_grant = roles_to_grant_or_revoke.getMatchingIDs();
        }
    }

    /// Checks if the current user has enough access rights granted with grant option to grant or revoke specified access rights.
    void checkGrantOption(
        const AccessControl & access_control,
        const ContextAccess & current_user_access,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed,
        const AccessRightsElements & elements_to_grant,
        AccessRightsElements & elements_to_revoke)
    {
        /// Check access rights which are going to be granted.
        /// To execute the command GRANT the current user needs to have the access granted with GRANT OPTION.
        current_user_access.checkGrantOption(elements_to_grant);

        if (current_user_access.hasGrantOption(elements_to_revoke))
        {
            /// Simple case: the current user has the grant option for all the access rights specified for REVOKE.
            return;
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have
        /// the access granted with GRANT OPTION but it's still ok because the roles or users
        /// from whom the access rights will be revoked don't have the specified access granted either.
        ///
        /// For example, to execute
        /// GRANT ALL ON mydb.* TO role1
        /// REVOKE ALL ON *.* FROM role1
        /// the current user needs to have grants only on the 'mydb' database.
        AccessRights all_granted_access;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *role);
                all_granted_access.makeUnion(role->access);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *user);
                all_granted_access.makeUnion(user->access);
            }
        }

        need_check_grantees_are_allowed = false; /// already checked

        if (!elements_to_revoke.empty() && elements_to_revoke[0].is_partial_revoke)
            std::for_each(elements_to_revoke.begin(), elements_to_revoke.end(), [&](AccessRightsElement & element) { element.is_partial_revoke = false; });
        AccessRights access_to_revoke;
        access_to_revoke.grant(elements_to_revoke);
        access_to_revoke.makeIntersection(all_granted_access);

        /// Build more accurate list of elements to revoke, now we use an intesection of the initial list of elements to revoke
        /// and all the granted access rights to these grantees.
        bool grant_option = !elements_to_revoke.empty() && elements_to_revoke[0].grant_option;
        elements_to_revoke.clear();
        for (auto & element_to_revoke : access_to_revoke.getElements())
        {
            if (!element_to_revoke.is_partial_revoke && (element_to_revoke.grant_option || !grant_option))
                elements_to_revoke.emplace_back(std::move(element_to_revoke));
        }

        current_user_access.checkGrantOption(elements_to_revoke);
    }

    /// Checks if the current user has enough roles granted with admin option to grant or revoke specified roles.
    void checkAdminOption(
        const AccessControl & access_control,
        const ContextAccess & current_user_access,
        const std::vector<UUID> & grantees_from_query,
        bool & need_check_grantees_are_allowed,
        const std::vector<UUID> & roles_to_grant,
        RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        /// Check roles which are going to be granted.
        /// To execute the command GRANT the current user needs to have the roles granted with ADMIN OPTION.
        current_user_access.checkAdminOption(roles_to_grant);

        /// Check roles which are going to be revoked.
        std::vector<UUID> roles_to_revoke_ids;
        if (!roles_to_revoke.all)
        {
            roles_to_revoke_ids = roles_to_revoke.getMatchingIDs();
            if (current_user_access.hasAdminOption(roles_to_revoke_ids))
            {
                /// Simple case: the current user has the admin option for all the roles specified for REVOKE.
                return;
            }
        }

        /// Special case for the command REVOKE: it's possible that the current user doesn't have the admin option
        /// for some of the specified roles but it's still ok because the roles or users from whom the roles will be
        /// revoked from don't have the specified roles granted either.
        ///
        /// For example, to execute
        /// GRANT role2 TO role1
        /// REVOKE ALL FROM role1
        /// the current user needs to have only 'role2' to be granted with admin option (not all the roles).
        GrantedRoles all_granted_roles;
        for (const auto & id : grantees_from_query)
        {
            auto entity = access_control.tryRead(id);
            if (auto role = typeid_cast<RolePtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *role);
                all_granted_roles.makeUnion(role->granted_roles);
            }
            else if (auto user = typeid_cast<UserPtr>(entity))
            {
                if (need_check_grantees_are_allowed)
                    current_user_access.checkGranteeIsAllowed(id, *user);
                all_granted_roles.makeUnion(user->granted_roles);
            }
        }

        need_check_grantees_are_allowed = false; /// already checked

        const auto & all_granted_roles_set = admin_option ? all_granted_roles.getGrantedWithAdminOption() : all_granted_roles.getGranted();
        if (roles_to_revoke.all)
            boost::range::set_difference(all_granted_roles_set, roles_to_revoke.except_ids, std::back_inserter(roles_to_revoke_ids));
        else
            boost::range::remove_erase_if(roles_to_revoke_ids, [&](const UUID & id) { return !all_granted_roles_set.count(id); });

        roles_to_revoke = roles_to_revoke_ids;
        current_user_access.checkAdminOption(roles_to_revoke_ids);
    }

    /// Returns access rights which should be checked for executing GRANT/REVOKE on cluster.
    /// This function is less accurate than checkGrantOption() because it cannot use any information about
    /// access rights the grantees currently have (due to those grantees are located on multiple nodes,
    /// we just don't have the full information about them).
    AccessRightsElements getRequiredAccessForExecutingOnCluster(const AccessRightsElements & elements_to_grant, const AccessRightsElements & elements_to_revoke)
    {
        auto required_access = elements_to_grant;
        required_access.insert(required_access.end(), elements_to_revoke.begin(), elements_to_revoke.end());
        std::for_each(required_access.begin(), required_access.end(), [&](AccessRightsElement & element) { element.grant_option = true; });
        return required_access;
    }

    /// Checks if the current user has enough roles granted with admin option to grant or revoke specified roles on cluster.
    /// This function is less accurate than checkAdminOption() because it cannot use any information about
    /// granted roles the grantees currently have (due to those grantees are located on multiple nodes,
    /// we just don't have the full information about them).
    void checkAdminOptionForExecutingOnCluster(const ContextAccess & current_user_access,
                                               const std::vector<UUID> roles_to_grant,
                                               const RolesOrUsersSet & roles_to_revoke)
    {
        if (roles_to_revoke.all)
        {
            /// Revoking all the roles on cluster always requires ROLE_ADMIN privilege
            /// because when we send the query REVOKE ALL to each shard we don't know at this point
            /// which roles exactly this is going to revoke on each shard.
            /// However ROLE_ADMIN just allows to revoke every role, that's why we check it here.
            current_user_access.checkAccess(AccessType::ROLE_ADMIN);
            return;
        }

        if (current_user_access.isGranted(AccessType::ROLE_ADMIN))
            return;

        for (const auto & role_id : roles_to_grant)
            current_user_access.checkAdminOption(role_id);


        for (const auto & role_id : roles_to_revoke.getMatchingIDs())
            current_user_access.checkAdminOption(role_id);
    }

    template <typename T>
    void updateGrantedAccessRightsAndRolesTemplate(
        T & grantee,
        const AccessRightsElements & elements_to_grant,
        const AccessRightsElements & elements_to_revoke,
        const std::vector<UUID> & roles_to_grant,
        const RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        if (!elements_to_revoke.empty())
            grantee.access.revoke(elements_to_revoke);

        if (!elements_to_grant.empty())
            grantee.access.grant(elements_to_grant);

        if (!roles_to_revoke.empty())
        {
            if (admin_option)
                grantee.granted_roles.revokeAdminOption(grantee.granted_roles.findGrantedWithAdminOption(roles_to_revoke));
            else
                grantee.granted_roles.revoke(grantee.granted_roles.findGranted(roles_to_revoke));
        }

        if (!roles_to_grant.empty())
        {
            if (admin_option)
                grantee.granted_roles.grantWithAdminOption(roles_to_grant);
            else
                grantee.granted_roles.grant(roles_to_grant);
        }
    }

    /// Updates grants of a specified user or role.
    void updateGrantedAccessRightsAndRoles(
        IAccessEntity & grantee,
        const AccessRightsElements & elements_to_grant,
        const AccessRightsElements & elements_to_revoke,
        const std::vector<UUID> & roles_to_grant,
        const RolesOrUsersSet & roles_to_revoke,
        bool admin_option)
    {
        if (auto * user = typeid_cast<User *>(&grantee))
            updateGrantedAccessRightsAndRolesTemplate(*user, elements_to_grant, elements_to_revoke, roles_to_grant, roles_to_revoke, admin_option);
        else if (auto * role = typeid_cast<Role *>(&grantee))
            updateGrantedAccessRightsAndRolesTemplate(*role, elements_to_grant, elements_to_revoke, roles_to_grant, roles_to_revoke, admin_option);
    }

    /// Updates grants of a specified user or role.
    void updateFromQuery(IAccessEntity & grantee, const ASTGrantQuery & query)
    {
        AccessRightsElements elements_to_grant, elements_to_revoke;
        collectAccessRightsElementsToGrantOrRevoke(query, elements_to_grant, elements_to_revoke);

        std::vector<UUID> roles_to_grant;
        RolesOrUsersSet roles_to_revoke;
        collectRolesToGrantOrRevoke(query, roles_to_grant, roles_to_revoke);

        updateGrantedAccessRightsAndRoles(grantee, elements_to_grant, elements_to_revoke, roles_to_grant, roles_to_revoke, query.admin_option);
    }
}


BlockIO InterpreterGrantQuery::execute()
{
    auto & query = query_ptr->as<ASTGrantQuery &>();

    query.replaceCurrentUserTag(getContext()->getUserName());
    query.access_rights_elements.eraseNonGrantable();

    if (!query.access_rights_elements.sameOptions())
        throw Exception("Elements of an ASTGrantQuery are expected to have the same options", ErrorCodes::LOGICAL_ERROR);
    if (!query.access_rights_elements.empty() && query.access_rights_elements[0].is_partial_revoke && !query.is_revoke)
        throw Exception("A partial revoke should be revoked, not granted", ErrorCodes::LOGICAL_ERROR);

    auto & access_control = getContext()->getAccessControl();
    auto current_user_access = getContext()->getAccess();

    std::vector<UUID> grantees = RolesOrUsersSet{*query.grantees, access_control, getContext()->getUserID()}.getMatchingIDs(access_control);

    /// Collect access rights and roles we're going to grant or revoke.
    AccessRightsElements elements_to_grant, elements_to_revoke;
    collectAccessRightsElementsToGrantOrRevoke(query, elements_to_grant, elements_to_revoke);

    std::vector<UUID> roles_to_grant;
    RolesOrUsersSet roles_to_revoke;
    collectRolesToGrantOrRevoke(access_control, query, roles_to_grant, roles_to_revoke);

    /// Executing on cluster.
    if (!query.cluster.empty())
    {
        auto required_access = getRequiredAccessForExecutingOnCluster(elements_to_grant, elements_to_revoke);
        checkAdminOptionForExecutingOnCluster(*current_user_access, roles_to_grant, roles_to_revoke);
        current_user_access->checkGranteesAreAllowed(grantees);
        DDLQueryOnClusterParams params;
        params.access_to_check = std::move(required_access);
        return executeDDLQueryOnCluster(query_ptr, getContext(), params);
    }

    /// Check if the current user has corresponding access rights granted with grant option.
    String current_database = getContext()->getCurrentDatabase();
    elements_to_grant.replaceEmptyDatabase(current_database);
    elements_to_revoke.replaceEmptyDatabase(current_database);
    bool need_check_grantees_are_allowed = true;
    checkGrantOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, elements_to_grant, elements_to_revoke);

    /// Check if the current user has corresponding roles granted with admin option.
    checkAdminOption(access_control, *current_user_access, grantees, need_check_grantees_are_allowed, roles_to_grant, roles_to_revoke, query.admin_option);

    if (need_check_grantees_are_allowed)
        current_user_access->checkGranteesAreAllowed(grantees);

    /// Update roles and users listed in `grantees`.
    auto update_func = [&](const AccessEntityPtr & entity) -> AccessEntityPtr
    {
        auto clone = entity->clone();
        updateGrantedAccessRightsAndRoles(*clone, elements_to_grant, elements_to_revoke, roles_to_grant, roles_to_revoke, query.admin_option);
        return clone;
    };

    access_control.update(grantees, update_func);

    return {};
}


void InterpreterGrantQuery::updateUserFromQuery(User & user, const ASTGrantQuery & query)
{
    updateFromQuery(user, query);
}

void InterpreterGrantQuery::updateRoleFromQuery(Role & role, const ASTGrantQuery & query)
{
    updateFromQuery(role, query);
}


void InterpreterGrantQuery::extendQueryLogElemImpl(QueryLogElement & elem, const ASTPtr & /*ast*/, ContextPtr) const
{
    auto & query = query_ptr->as<ASTGrantQuery &>();
    if (query.is_revoke)
        elem.query_kind = "Revoke";
    else
        elem.query_kind = "Grant";
}

}
