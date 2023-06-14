#pragma once

#include <Parsers/IAST.h>
#include <Access/Common/AccessRightsElement.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{
class ASTRolesOrUsersSet;


/** GRANT access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user_name | CURRENT_USER} [,...] [WITH GRANT OPTION]
  * REVOKE access_type[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | CURRENT_USER} [,...]
  *
  * GRANT role [,...] TO {user_name | role_name | CURRENT_USER} [,...] [WITH ADMIN OPTION]
  * REVOKE [ADMIN OPTION FOR] role [,...] FROM {user_name | role_name | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
  */
class ASTGrantQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    /// It's allowed to grant and revoke in the same command.
    /// GRANT OPTION can be specified inside those elements.
    AccessRightsElements rights_to_grant_and_revoke;

    /// A single GRANT or REVOKE command can grant either rights or roles but not rights and roles together.
    std::shared_ptr<ASTRolesOrUsersSet> roles_to_revoke;
    std::shared_ptr<ASTRolesOrUsersSet> roles_to_revoke_admin_option;
    std::shared_ptr<ASTRolesOrUsersSet> roles_to_grant_with_admin_option;
    std::shared_ptr<ASTRolesOrUsersSet> roles_to_grant;

    /// Recipients of the GRANT or REVOKE command.
    std::shared_ptr<ASTRolesOrUsersSet> grantees;

    bool replace_option = false;
    bool attach_mode = false;

    void setCurrentDatabase(const String & current_database);
    void setCurrentUser(const String & current_user_name);

    QueryKind getQueryKind() const override;
    String getID(char) const override;
    ASTPtr clone() const override;
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTGrantQuery>(clone()); }
};
}
