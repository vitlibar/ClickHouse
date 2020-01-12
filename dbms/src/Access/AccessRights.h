#pragma once

#include <Core/Types.h>
#include <Access/AccessType.h>
#include <memory>
#include <vector>


namespace DB
{
/// Represents a set of access types granted on databases, tables, columns, etc.
/// For example, "GRANT SELECT, UPDATE ON db.*, GRANT INSERT ON db2.mytbl2" are access rights.
class AccessRights
{
public:
    AccessRights();
    AccessRights(const AccessType & access);
    ~AccessRights();
    AccessRights(const AccessRights & src);
    AccessRights & operator =(const AccessRights & src);
    AccessRights(AccessRights && src);
    AccessRights & operator =(AccessRights && src);

    bool isEmpty() const;

    /// Revokes everything. It's the same as fullRevoke(AccessType::ALL).
    void clear();

    /// Grants access on a specified database/table/column.
    /// Does nothing if the specified access has been already granted.
    void grant(const AccessType & access);
    void grant(const AccessType & access, const std::string_view & database);
    void grant(const AccessType & access, const Strings & databases);
    void grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary);
    void grant(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries);
    void grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute);
    void grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes);

    /// Revokes a specified access granted before on a specified database/table/column.
    /// Does nothing if the specified access isn't granted.
    /// If the specified access is granted but on upper (e.g. database for table, table for columns)
    /// or lower level, the function also does nothing.
    /// This function implements the standard SQL REVOKE behaviour.
    void revoke(const AccessType & access);
    void revoke(const AccessType & access, const std::string_view & database);
    void revoke(const AccessType & access, const Strings & databases);
    void revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary);
    void revoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries);
    void revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute);
    void revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes);

    /// Revokes a specified access granted before on a specified database/table/column.
    /// The function also restricts access if it's granted on upper level.
    /// For example, an access could be granted on a database and then revoked on a table in this database.
    /// This function implements the MySQL REVOKE behaviour with partial_revokes is ON.
    void partialRevoke(const AccessType & access);
    void partialRevoke(const AccessType & access, const std::string_view & database);
    void partialRevoke(const AccessType & access, const Strings & databases);
    void partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary);
    void partialRevoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries);
    void partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute);
    void partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes);

    /// Revokes a specified access granted before on a specified database/table/column or on lower levels.
    /// The function also restricts access if it's granted on upper level.
    /// For example, fullRevoke(AccessType::ALL) revokes all grants at all, just like clear();
    /// fullRevoke(AccessType::SELECT, db) means it's not allowed to execute SELECT in that database anymore (from any table).
    void fullRevoke(const AccessType & access);
    void fullRevoke(const AccessType & access, const std::string_view & database);
    void fullRevoke(const AccessType & access, const Strings & databases);
    void fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary);
    void fullRevoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries);
    void fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute);
    void fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes);

    struct Info
    {
        AccessType access;
        String database;
        String table_or_dictionary;
        String column_or_attribute;

        enum class Kind
        {
            GRANT,
            PARTIAL_REVOKE,
        };
        Kind kind = Kind::GRANT;

        /// Outputs an info to a string in readable format, for example "GRANT SELECT(id, name), INSERT ON mydatabase.*".
        String toString() const;
    };

    /// Returns the information about all the access granted.
    std::vector<Info> getInfos() const;

    /// Returns the information about all the access granted as a string.
    String toString() const;

    /// Whether a specified access granted.
    bool isGranted(const AccessType & access) const;
    bool isGranted(const AccessType & access, const std::string_view & database) const;
    bool isGranted(const AccessType & access, const Strings & databases) const;
    bool isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) const;
    bool isGranted(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) const;
    bool isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) const;
    bool isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) const;

    /// Returns the access granted on a specified database/table/column.
    AccessType getAccess() const;
    AccessType getAccess(const std::string_view & database) const;
    AccessType getAccess(const Strings & databases) const;
    AccessType getAccess(const std::string_view & database, const std::string_view & table_or_dictionary) const;
    AccessType getAccess(const std::string_view & database, const Strings & tables_or_dictionaries) const;
    AccessType getAccess(const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) const;
    AccessType getAccess(const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) const;

    friend bool operator ==(const AccessRights & left, const AccessRights & right);
    friend bool operator !=(const AccessRights & left, const AccessRights & right) { return !(left == right); }

    /// Merges two sets of access rights together.
    /// It's used to combine access rights from multiple roles.
    void merge(const AccessRights & other);

    /// (Helper function)
    /// Outputs information about a granted access into string.
    static String grantToString(const AccessType & access);
    static String grantToString(const AccessType & access, const std::string_view & database);
    static String grantToString(const AccessType & access, const Strings & databases);
    static String grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary);
    static String grantToString(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries);
    static String grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute);
    static String grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes);

private:
    template <typename... Args>
    void grantImpl(const AccessType & access, const Args &... args);

    template <typename... Args>
    void revokeImpl(const AccessType & access, const Args &... args);

    template <typename... Args>
    void partialRevokeImpl(const AccessType & access, const Args &... args);

    template <typename... Args>
    void fullRevokeImpl(const AccessType & access, const Args &... args);

    template <typename... Args>
    bool isGrantedImpl(const AccessType & access, const Args &... args) const;

    template <typename... Args>
    AccessType getAccessImpl(const Args &... args) const;

    struct Node;
    std::unique_ptr<Node> root;
};

}
