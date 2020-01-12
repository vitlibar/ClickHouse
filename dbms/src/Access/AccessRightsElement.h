#pragma once

#include <Access/AccessFlags.h>


namespace DB
{
/// An element of access rights which can be represented by single line
/// GRANT ... ON ...
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

    /// Replace the database used in this element with `new_database`.
    void setDatabase(const String & new_database);
    void setDatabase(CurrentDatabaseTag new_database);

    /// If the database used in this element is `old_database`, replaces it with `new_database`.
    /// Otherwise does nothing.
    void replaceDatabase(const String & old_database, const String & new_database);
    void replaceDatabase(CurrentDatabaseTag old_database, const String & new_database);
    void replaceDatabase(const String & old_database, CurrentDatabaseTag new_database);

    /// Returns a human-readable representation like "SELECT, UPDATE(x, y) ON db.table".
    /// The returned string isn't prefixed with the "GRANT" keyword.
    String toString() const;
};


/// Multiple elements of access rights.
class AccessRightsElements : public std::vector<AccessRightsElement>
{
public:
    using CurrentDatabaseTag = AccessRightsElement::CurrentDatabaseTag;

    /// If the database used in this element is `old_database`, replaces it with `new_database`.
    /// Otherwise does nothing.
    void replaceDatabase(const String & old_database, const String & new_database);
    void replaceDatabase(CurrentDatabaseTag old_database, const String & new_database);
    void replaceDatabase(const String & old_database, CurrentDatabaseTag new_database);

    /// Returns a human-readable representation like "SELECT, UPDATE(x, y) ON db.table".
    /// The returned string isn't prefixed with the "GRANT" keyword.
    String toString() const;
};

}
