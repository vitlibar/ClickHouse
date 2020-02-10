#pragma once

#include <Core/UUID.h>
#include <memory>
#include <unordered_set>


namespace DB
{
class ASTRoleList;
class AccessControlManager;


/// Represents a list of users/roles like
/// {user_name | role_name | CURRENT_USER} [,...] | NONE | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
/// Similar to ASTRoleList, but with IDs instead of names.
class RoleList
{
public:
    RoleList();
    RoleList(const RoleList & src);
    RoleList & operator =(const RoleList & src);
    RoleList(RoleList && src);
    RoleList & operator =(RoleList && src);

    RoleList(const UUID & id);
    RoleList(const std::vector<UUID> & ids_);

    RoleList(const ASTRoleList & ast, const AccessControlManager & manager, const UUID & current_user_id);
    std::shared_ptr<ASTRoleList> toAST(const AccessControlManager & manager) const;

    String toString(const AccessControlManager & manager) const;
    Strings toStrings(const AccessControlManager & manager) const;

    bool empty() const;
    void clear();
    void add(const UUID & id);
    void add(const std::vector<UUID> & ids_);

    bool match(const UUID & user_id) const;
    std::vector<UUID> getAllMatchingUsers(const AccessControlManager & manager) const;

    friend bool operator ==(const RoleList & lhs, const RoleList & rhs);
    friend bool operator !=(const RoleList & lhs, const RoleList & rhs) { return !(lhs == rhs); }

private:
    std::unordered_set<UUID> ids;
    bool all = false;
    std::unordered_set<UUID> except_ids;
};

}
