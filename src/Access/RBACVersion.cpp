#include <base/types.h>

namespace DB
{

extern const UInt64 RBAC_INITIAL_VERSION = 1;

/// rbac_version < 2:
/// row policies are permissive by default
///
/// rbac_version >= 2:
/// row policies are simple by default
extern const UInt64 RBAC_VERSION_ROW_POLICIES_ARE_SIMPLE_BY_DEFAULT = 2;

extern const UInt64 RBAC_LATEST_VERSION = 2;

const char * RBAC_VERSION_SETTING_NAME = "rbac_version";

}
