#pragma once

#include <Access/IAccessEntity.h>
#include <Access/AccessRights.h>
#include <Core/Types.h>
#include <Core/UUID.h>


namespace DB
{

struct Role : public IAccessEntity
{
    AccessRights access;
    AccessRights access_with_grant_option;
    std::vector<UUID> granted_roles;
    std::vector<UUID> granted_roles_with_admin_option;

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<Role>(); }
};

using RolePtr = std::shared_ptr<const Role>;
}
