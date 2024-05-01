#pragma once

#include <Core/UUID.h>


namespace DB
{
class ASTCreateQuery;
enum class TargetTableKind;

/// UUID of the table or the database which is defined with a CREATE QUERY combined with UUIDs of its inner tables.
struct CreateQueryUUIDs
{
    UUID uuid = UUIDHelpers::Nil;
    std::vector<std::pair<TargetTableKind, UUID>> inner_uuids;

    CreateQueryUUIDs() = default;
    explicit CreateQueryUUIDs(const ASTCreateQuery & query);

    bool empty() const;

    String toString() const;
    static CreateQueryUUIDs fromString(const String & str);
};

}
