#pragma once

#include <Core/BaseSettings.h>


namespace DB
{

#define LIST_OF_RESTORE_FROM_BACKUP_SETTINGS(M) \
    M(Bool, skip_existing_tables, false, "", 0) \
    M(Bool, restore_into_non_empty_tables, false, "", 0) \
    M(Bool, restore_into_non_empty_partitions, false, "", 0) \

DECLARE_SETTINGS_TRAITS(RestoreFromBackupSettingsTraits, LIST_OF_RESTORE_FROM_BACKUP_SETTINGS)

struct RestoreFromBackupSettings : public BaseSettings<RestoreFromBackupSettingsTraits> {};

}
