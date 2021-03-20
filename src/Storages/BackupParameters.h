#pragma once

#include <Core/Types.h>


namespace DB
{
using Strings = std::vector<std::string>;

/// Parameters for creating backups.
struct BackupParameters
{
    /// If not empty then only the specified databases will be stored.
    Strings databases;

    /// If not empty then only the specified tables will be stored.
    Strings table_names;

    /// If not empty then only the specified partitions will be stored.
    Strings partitions;
};

/// Parameters for restoring from backups.
struct RestoreParameters
{
    /// If not empty then only the specified databases will be restored.
    Strings databases;

    /// If not empty then only the specified tables will be restored.
    Strings table_names;

    /// If not empty then only the specified partitions will be restored.
    Strings partitions;

    /// If not empty then database/table will be restored with another name.
    String new_database_name;
    String new_table_name;
    String new_partition_name;
};

}
