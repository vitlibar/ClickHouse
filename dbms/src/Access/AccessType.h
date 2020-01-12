#pragma once

#include <Core/Types.h>
#include <bitset>


namespace DB
{
/// Represents an access type which can be granted on databases, tables, columns, etc.
/// For example "SELECT, CREATE USER" is an access type.
class AccessType
{
public:
    enum Type
    {
        NONE,  /// No access
        ALL,   /// Full access.

        SELECT, /// User can execute SELECT on tables.
        INSERT, /// User can execute INSERT on tables.
        UPDATE, /// User can execute ALTER UPDATE on tables.
        DELETE, /// User can execute ALTER DELETE on tables.
    };

    AccessType(Type type);

    /// The same as AccessType(Type::NONE).
    AccessType() = default;

    /// Constructs from a string like "SELECT" or "ALL PRIVILEGES EXCEPT UPDATE AND DELETE".
    AccessType(const std::string_view & keyword);

    /// Constructs from a list of strings like "SELECT, UPDATE, INSERT".
    AccessType(const std::vector<std::string_view> & keywords);
    AccessType(const Strings & keywords);

    AccessType(const AccessType & src) = default;
    AccessType(AccessType && src) = default;
    AccessType & operator =(const AccessType & src) = default;
    AccessType & operator =(AccessType && src) = default;

    /// Returns the access type which contains two specified access types.
    AccessType & operator |=(const AccessType & other) { flags |= other.flags; return *this; }
    friend AccessType operator |(const AccessType & left, const AccessType & right) { return AccessType(left) |= right; }

    /// Returns the access type which contains the common part of two access types.
    AccessType & operator &=(const AccessType & other) { flags &= other.flags; return *this; }
    friend AccessType operator &(const AccessType & left, const AccessType & right) { return AccessType(left) &= right; }

    /// Returns the access type which contains only the part of the first access type which is not the part of the second access type.
    /// (lhs - rhs) is the same as (lhs & ~rhs).
    AccessType & operator -=(const AccessType & other) { flags &= ~other.flags; return *this; }
    friend AccessType operator -(const AccessType & left, const AccessType & right) { return AccessType(left) -= right; }

    AccessType operator ~() const { AccessType res; res.flags = ~flags; return res; }

    bool isEmpty() const { return flags.none(); }
    explicit operator bool() const { return isEmpty(); }
    bool contains(const AccessType & other) const { return (flags & other.flags) == other.flags; }

    friend bool operator ==(const AccessType & left, const AccessType & right) { return left.flags == right.flags; }
    friend bool operator !=(const AccessType & left, const AccessType & right) { return !(left == right); }

    void clear() { flags.reset(); }

    /// Returns a comma-separated list of keywords, like "SELECT, CREATE USER, UPDATE".
    String toString() const;

    /// Returns a list of keywords.
    Strings toKeywords() const;

    /// Checks that this access type can be granted on the database level.
    /// For example, SELECT can be granted on the database level, but CREATE_USER cannot.
    void checkGrantableOnDatabaseLevel() const;

    /// Checks that this access type can be granted on the table/dictionary level.
    void checkGrantableOnTableLevel() const;

    /// Checks that this access type can be granted on the column/attribute level.
    void checkGrantableOnColumnLevel() const;

private:
    static constexpr size_t NUM_FLAGS = 64;
    std::bitset<NUM_FLAGS> flags;
};

}
