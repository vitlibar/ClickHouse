#pragma once


namespace DB
{
    class RoleList
    {
        RoleList();
        RoleList(const ASTRoleList & ast, UUID current_user_id = {});

        RoleList(const RoleList & src);
        RoleList & operator =(const RoleList & src);
        RoleList(RoleList && src);
        RoleList & operator =(RoleList && src);

        std::shared_ptr<ASTRoleList> toAST(const AccessControlManager & manager) const;

        bool match(const UUID & user_id, const std::shared_ptr<const std::vector<EnabledRole>> & enabled_roles) const;
        static std::vector<UUID> findAllMatchingUsers(const AccessControlManager & manager, const ASTRoleList & ast, UUID current_user_id = {});

        friend bool operator ==(const RoleList & lhs, const RoleList & rhs);
        friend bool operator !=(const RoleList & lhs, const RoleList & rhs);

    private:
        std::vector<UUID> ids;
        bool all = false;
        std::vector<UUID> except_ids;
    };
}
