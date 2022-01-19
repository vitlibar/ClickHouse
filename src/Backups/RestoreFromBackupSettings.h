#pragma once

#include <Core/BaseSettings.h>


namespace DB
{

enum class CreateOrNotCreateRestoreFlag
{
    kYes,
    kNo,
    kIfNotExists,
};

DECLARE_SETTING_ENUM(SettingFieldCreateOrNotCreateRestoreFlag, CreateOrNotCreateRestoreFlag)

enum class DataDeduplicationOnFailureRestoreMode
{
    kThrow,
    kSkip,
    kInsert,
};

DECLARE_SETTING_ENUM(SettingFieldDataDeduplicationOnFailureRestoreMode, DataDeduplicationOnFailureRestoreMode)


#define LIST_OF_RESTORE_FROM_BACKUP_SETTINGS(M) \
    M(CreateOrNotCreateRestoreFlag, create_tables, CreateOrNotCreateRestoreFlag::kIfNotExists, "", 0) \
    M(CreateOrNotCreateRestoreFlag, create_databases, CreateOrNotCreateRestoreFlag::kIfNotExists, "", 0) \
    M(CreateOrNotCreateRestoreFlag, create_databases_implicitly, CreateOrNotCreateRestoreFlag::kIfNotExists, "", 0) \

    M(Bool, data_deduplication, true, "", 0) \
    M(DataDeduplicationOnFailureRestoreMode, data_deduplication_on_failure, DataDeduplicationOnFailureRestoreMode::kThrow, "", 0) \

    M(Bool, throw_if_table_not_empty, false, "", 0) \
    M(Bool, throw_if_partition_not_empty, false, "", 0) \

DECLARE_SETTINGS_TRAITS(RestoreFromBackupSettingsTraits, LIST_OF_RESTORE_FROM_BACKUP_SETTINGS)

struct RestoreFromBackupSettings : public BaseSettings<RestoreFromBackupSettingsTraits> {};

}
