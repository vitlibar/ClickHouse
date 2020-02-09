#pragma once

#include <Core/UUID.h>


namespace DB
{
struct Role;
using RolePtr = std::shared_ptr<const Role>;

struct EnabledRole
{
    RolePtr role;
    UUID id{UInt128(0)};
    bool admin_option = false;
};

using EnabledRoles = std::vector<EnabledRole>;
using EnabledRolesPtr = std::shared_ptr<const EnabledRoles>;
}
