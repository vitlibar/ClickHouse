#pragma once

#include <Access/IAccessEntity.h>
#include <Access/Authentication.h>
#include <Access/AllowedClientHosts.h>
#include <Access/AccessRights.h>
#include <Core/Types.h>
#include <Core/UUID.h>


namespace DB
{
/** User and ACL.
  */
struct User : public IAccessEntity
{
    Authentication authentication;
    AllowedClientHosts allowed_client_hosts{AllowedClientHosts::AnyHostTag{}};
    AccessRights access;
    AccessRights access_with_grant_option;
    std::vector<UUID> granted_roles;
    std::vector<UUID> granted_roles_with_admin_option;
    std::vector<UUID> default_roles;
    String profile;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<User>(); }
};

using UserPtr = std::shared_ptr<const User>;
}
