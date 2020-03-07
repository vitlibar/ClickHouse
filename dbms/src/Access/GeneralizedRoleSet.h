#pragma once

#include <Core/UUID.h>
#include <boost/container/flat_set.hpp>
#include <memory>
#include <optional>


namespace DB
{
class ASTGeneralizedRoleSet;
class AccessControlManager;


/// Represents a set of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
/// Similar to ASTGenericRoleSet, but with IDs instead of names.
struct GeneralizedRoleSet
{
    GeneralizedRoleSet();
    GeneralizedRoleSet(const GeneralizedRoleSet & src);
    GeneralizedRoleSet & operator =(const GeneralizedRoleSet & src);
    GeneralizedRoleSet(GeneralizedRoleSet && src);
    GeneralizedRoleSet & operator =(GeneralizedRoleSet && src);

    struct AllTag {};
    GeneralizedRoleSet(AllTag);

    GeneralizedRoleSet(const UUID & id);
    GeneralizedRoleSet(const std::vector<UUID> & ids_);
    GeneralizedRoleSet(const boost::container::flat_set<UUID> & ids_);

    /// The constructor from AST requires the AccessControlManager if `ast.id_mode == false`.
    GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast);
    GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const UUID & current_user_id);
    GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const AccessControlManager & manager);
    GeneralizedRoleSet(const ASTGeneralizedRoleSet & ast, const AccessControlManager & manager, const UUID & current_user_id);

    std::shared_ptr<ASTGeneralizedRoleSet> toAST() const;
    String toString() const;
    Strings toStrings() const;

    std::shared_ptr<ASTGeneralizedRoleSet> toASTWithNames(const AccessControlManager & manager) const;
    String toStringWithNames(const AccessControlManager & manager) const;
    Strings toStringsWithNames(const AccessControlManager & manager) const;

    bool empty() const;
    void clear();
    void add(const UUID & id);
    void add(const std::vector<UUID> & ids_);
    void add(const boost::container::flat_set<UUID> & ids_);

    /// Checks if a specified ID matches this GenericRoleSet.
    bool match(const UUID & id) const;
    bool match(const UUID & user_id, const std::vector<UUID> & enabled_roles) const;
    bool match(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles) const;

    /// Returns a list of matching IDs. The function must not be called if `all` == `true`.
    std::vector<UUID> getMatchingIDs() const;

    /// Returns a list of matching users.
    std::vector<UUID> getMatchingUsers(const AccessControlManager & manager) const;
    std::vector<UUID> getMatchingRoles(const AccessControlManager & manager) const;
    std::vector<UUID> getMatchingUsersAndRoles(const AccessControlManager & manager) const;

    friend bool operator ==(const GeneralizedRoleSet & lhs, const GeneralizedRoleSet & rhs);
    friend bool operator !=(const GeneralizedRoleSet & lhs, const GeneralizedRoleSet & rhs) { return !(lhs == rhs); }

    boost::container::flat_set<UUID> ids;
    bool all = false;
    boost::container::flat_set<UUID> except_ids;

private:
    void init(const ASTGeneralizedRoleSet & ast, const AccessControlManager * manager = nullptr, const UUID * current_user_id = nullptr);
};

}
