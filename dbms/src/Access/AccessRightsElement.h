#pragma once

#include <Access/AccessFlags.h>


namespace DB
{
struct AccessRightsElement
{
    AccessFlags access_flags;
    String database;
    String table;
    Strings columns;
    bool any_database = true;
    bool current_database = false;
    bool any_table = true;
    bool any_column = true;

    struct CurrentDatabaseTag {};

    AccessRightsElement() = default;
    AccessRightsElement(const AccessRightsElement &) = default;
    AccessRightsElement & operator =(const AccessRightsElement &) = default;
    AccessRightsElement(AccessRightsElement &&) = default;
    AccessRightsElement & operator =(AccessRightsElement &&) = default;

    AccessRightsElement(AccessFlags access_flags_) : access_flags(access_flags_) {}
    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_);
    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_);
    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const std::string_view & column_);
    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const std::vector<std::string_view> & columns_);
    AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const Strings & columns_);
    AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag) : access_flags(access_flags_), any_database(false), current_database(true) {}
    AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_);
    AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const std::string_view & column_);
    AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const std::vector<std::string_view> & columns_);
    AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const Strings & columns_);

    void setDatabase(const String & database_);
    void setDatabase(CurrentDatabaseTag);
    void replaceDatabase(const String & old_database_, const String & new_database_);
    void replaceDatabase(CurrentDatabaseTag old_database_, const String & new_database_);
    void replaceDatabase(const String & old_database_, CurrentDatabaseTag new_database_);

    String toString() const;
};


class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    using CurrentDatabaseTag = AccessRightsElement::CurrentDatabaseTag;

    void replaceDatabase(const String & old_database_, const String & new_database_);
    void replaceDatabase(CurrentDatabaseTag old_database_, const String & new_database_);
    void replaceDatabase(const String & old_database_, CurrentDatabaseTag new_database_);

    String toString() const;
};

}
