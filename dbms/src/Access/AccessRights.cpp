#include <Access/AccessRights.h>
#include <Common/quoteString.h>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/join.hpp>
#include <array>
#include <unordered_map>


namespace DB
{
namespace
{
    using Kind = AccessRights::Info::Kind;

    String kindToString(Kind kind)
    {
        return (kind == Kind::GRANT) ? "GRANT" : "REVOKE";
    }

    enum class RevokeMode
    {
        NORMAL,  /// for AccessRights::revoke()
        PARTIAL, /// for AccessRights::partialRevoke()
        FULL,    /// for AccessRights::fullRevoke()
    };

    auto toIterable(const std::string_view & str) { return std::array<std::string_view, 1>{str}; }
    const auto & toIterable(const Strings & vec) { return vec; }

    String grantToStringImpl(Kind kind, const AccessType & access)
    {
        return kindToString(kind) + " " + access.toString() + " ON *.*";
    }

    template <typename StringListT>
    String grantToStringImpl(Kind kind, const AccessType & access, const StringListT & databases)
    {
        String msg = kindToString(kind) + " " + access.toString() + " ON ";
        bool need_comma = false;
        for (const auto & database : toIterable(databases))
        {
            if (need_comma)
                msg += ", ";
            msg += backQuoteIfNeed(database) + ".*";
            need_comma = true;
        }
        return msg;
    }

    template <typename StringListT>
    String grantToStringImpl(Kind kind, const AccessType & access, const std::string_view & database, const StringListT & tables_or_dictionaries)
    {
        String msg = kindToString(kind) + " " + access.toString() + " ON ";
        bool need_comma = false;
        for (const auto & table_or_dictionary : toIterable(tables_or_dictionaries))
        {
            if (need_comma)
                msg += ", ";
            msg += backQuoteIfNeed(database) + "." + backQuoteIfNeed(table_or_dictionary);
            need_comma = true;
        }
        return msg;
    }

    template <typename StringListT>
    String grantToStringImpl(
        Kind kind,
        const AccessType & access,
        const std::string_view & database,
        const std::string_view & table_or_dictionary,
        const StringListT & columns_or_attributes)
    {
        String columns_in_parentheses;
        for (const auto & column_or_attribute : toIterable(columns_or_attributes))
        {
            columns_in_parentheses += columns_in_parentheses.empty() ? "(" : ", ";
            columns_in_parentheses += backQuoteIfNeed(std::string_view{column_or_attribute});
        }
        columns_in_parentheses += ")";
        String msg;
        for (const String & keyword : access.toKeywords())
            msg += (msg.empty() ? (kindToString(kind) + " ") : String{", "}) + keyword + columns_in_parentheses;
        msg += " ON " + backQuoteIfNeed(database) + "." + backQuoteIfNeed(table_or_dictionary);
        return msg;
    }

    template <typename... Args>
    String grantToStringImpl(const AccessType & access, const Args &... args)
    {
        return grantToStringImpl(Kind::GRANT, access, args...);
    }
}


struct AccessRights::Node
{
public:
    std::shared_ptr<const String> node_name;
    AccessType explicit_grants;
    AccessType inherited_access;
    AccessType partial_revokes;
    AccessType access; /// access == (inherited_access - partial_revokes) | explicit_grants
    std::unique_ptr<std::unordered_map<std::string_view, Node>> children;

    Node() = default;
    Node(Node && src) = default;
    Node & operator =(Node && src) = default;

    Node(const Node & src) { *this = src; }

    Node & operator =(const Node & src)
    {
        node_name = src.node_name;
        access = src.access;
        explicit_grants = src.explicit_grants;
        inherited_access = src.inherited_access;
        partial_revokes = src.partial_revokes;
        if (src.children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    void grant(const AccessType & access_)
    {
        explicit_grants |= access_;
        static constexpr void (*traverse_nodes)(Node &, const AccessType &) = [](Node & node, const AccessType & add_access)
        {
            node.access |= add_access;
            node.partial_revokes -= add_access;
            if (node.children)
            {
                for (auto & child : *node.children | boost::adaptors::map_values)
                {
                    child.inherited_access |= add_access;
                    traverse_nodes(child, add_access);
                }
            }
        };
        traverse_nodes(*this, access_);
    }

    template <typename StringListT>
    void grant(const AccessType & access_, const StringListT & names)
    {
        for (const auto & name : toIterable(names))
            getChild(name).grant(access_);
    }

    template <typename StringListT>
    void grant(const AccessType & access_, const std::string_view & name, const StringListT & subnames)
    {
        getChild(name).grant(access_, subnames);
    }

    template <typename StringListT>
    void grant(const AccessType & access_, const std::string_view & name, const std::string_view & subname, const StringListT & subsubnames)
    {
        getChild(name).grant(access_, subname, subsubnames);
    }

    template <RevokeMode revoke_mode>
    void revoke(const AccessType & access_)
    {
        if constexpr (revoke_mode == RevokeMode::NORMAL)
        {
            explicit_grants -= access_;
            static constexpr void (*traverse_nodes)(Node &) = [](Node & node)
            {
                node.partial_revokes &= node.inherited_access;
                node.access = (node.inherited_access - node.partial_revokes) | node.explicit_grants;
                if (node.children)
                {
                    for (auto & child : *node.children | boost::adaptors::map_values)
                    {
                        child.inherited_access = node.access;
                        traverse_nodes(child);
                    }
                }
            };
            traverse_nodes(*this);
        }
        else if constexpr (revoke_mode == RevokeMode::PARTIAL)
        {
            explicit_grants -= access_;
            partial_revokes |= access_;
            static constexpr void (*traverse_nodes)(Node &) = [](Node & node)
            {
                node.partial_revokes &= node.inherited_access;
                node.access = (node.inherited_access - node.partial_revokes) | node.explicit_grants;
                if (node.children)
                {
                    for (auto & child : *node.children | boost::adaptors::map_values)
                    {
                        child.inherited_access = node.access;
                        traverse_nodes(child);
                    }
                }
            };
            traverse_nodes(*this);
        }
        else /// revoke_mode == RevokeMode::FULL
        {
            if (access_ == AccessType::ALL)
            {
                partial_revokes = inherited_access;
                explicit_grants = AccessType::NONE;
                access = AccessType::NONE;
                children.reset();
            }
            else
            {
                partial_revokes |= access_;
                static constexpr void (*traverse_nodes)(Node &, const AccessType &) = [](Node & node, const AccessType & remove_access)
                {
                    node.partial_revokes &= node.inherited_access;
                    node.explicit_grants -= remove_access;
                    node.access -= remove_access;
                    if (node.children)
                    {
                        for (auto & child : *node.children | boost::adaptors::map_values)
                        {
                            child.inherited_access = node.access;
                            traverse_nodes(child, remove_access);
                        }
                    }
                };
                traverse_nodes(*this, access_);
            }
        }
    }

    template <RevokeMode revoke_mode, typename StringListT>
    void revoke(const AccessType & access_, const StringListT & names)
    {
        for (const auto & name : toIterable(names))
        {
            Node * child = (revoke_mode == RevokeMode::NORMAL) ? tryGetChild(name) : &getChild(name);
            if (child)
                child->revoke<revoke_mode>(access_);
        }
    }

    template <RevokeMode revoke_mode, typename StringListT>
    void revoke(const AccessType & access_, const std::string_view & name, const StringListT & subnames)
    {
        Node * child = (revoke_mode == RevokeMode::NORMAL) ? tryGetChild(name) : &getChild(name);
        if (child)
            child->revoke<revoke_mode>(access_, subnames);
    }

    template <RevokeMode revoke_mode, typename StringListT>
    void revoke(const AccessType & access_, const std::string_view & name, const std::string_view & subname, const StringListT & subsubnames)
    {
        Node * child = (revoke_mode == RevokeMode::NORMAL) ? tryGetChild(name) : &getChild(name);
        if (child)
            child->revoke<revoke_mode>(access_, subname, subsubnames);
    }

    const AccessType & getAccess() const { return access; }

    template <typename StringListT>
    AccessType getAccess(const StringListT & names) const
    {
        AccessType r = AccessType::ALL;
        for (const auto & name : toIterable(names))
            r &= getAccess(name);
        return r;
    }

    template <typename StringListT>
    AccessType getAccess(const std::string_view & name, const StringListT & subnames) const
    {
        const auto * child = tryGetChild(name);
        return child ? child->getAccess(subnames) : getAccess();
    }

    template <typename StringListT>
    AccessType getAccess(const std::string_view & name, const std::string_view & subname, const StringListT & subsubnames) const
    {
        const auto * child = tryGetChild(name);
        return child ? child->getAccess(subname, subsubnames) : getAccess();
    }

    void merge(const Node & other)
    {
        static constexpr void (*calc_grants)(Node & lhs, const Node & rhs) = [](Node & lhs, const Node & rhs)
        {
            lhs.access |= rhs.access;
            lhs.inherited_access |= rhs.inherited_access;
            lhs.explicit_grants = lhs.access - lhs.inherited_access;
            lhs.partial_revokes = lhs.inherited_access - lhs.access;
        };
        static constexpr void (*traverse_nodes)(Node & lhs, const Node & rhs) = [](Node & lhs, const Node & rhs)
        {
            calc_grants(lhs, rhs);
            if (rhs.children)
            {
                for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                    traverse_nodes(lhs.getChild(rhs_childname), rhs_child);
            }
            if (lhs.children)
            {
                for (auto & [lhs_childname, lhs_child] : *lhs.children)
                    if (!rhs.tryGetChild(lhs_childname))
                        calc_grants(lhs_child, rhs);
            }
        };
        traverse_nodes(*this, other);
    }

    Node * tryGetChild(const std::string_view & name)
    {
        if (!children)
            return nullptr;
        auto it = children->find(name);
        if (it == children->end())
            return nullptr;
        return &it->second;
    }

    const Node * tryGetChild(const std::string_view & name) const
    {
        if (!children)
            return nullptr;
        auto it = children->find(name);
        if (it == children->end())
            return nullptr;
        return &it->second;
    }

    Node & getChild(const std::string_view & name)
    {
        auto * child = tryGetChild(name);
        if (child)
            return *child;
        if (!children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>();
        auto new_child_name = std::make_shared<String>(name);
        Node & new_child = (*children)[*new_child_name];
        new_child.node_name = std::move(new_child_name);
        new_child.inherited_access = access;
        new_child.access = access;
        return new_child;
    }

    bool isEmpty() const
    {
        if (explicit_grants || partial_revokes)
            return false;
        if (children)
        {
            for (const auto & child : *children | boost::adaptors::map_values)
                if (!child.isEmpty())
                    return false;
        }
        return true;
    }

    friend bool operator ==(const Node & left, const Node & right)
    {
        if ((left.explicit_grants != right.explicit_grants) || (left.partial_revokes != right.partial_revokes))
            return false;

        if (left.children)
        {
            for (const auto & [left_child_name, left_child] : *left.children)
            {
                const auto * right_child = right.tryGetChild(left_child_name);
                if (right_child)
                {
                    if (left_child != *right_child)
                        return false;
                }
                else
                {
                    if (!left_child.isEmpty())
                        return false;
                }
            }
        }

        if (right.children)
        {
            for (const auto & [right_child_name, right_child] : *right.children)
            {
                if (!left.tryGetChild(right_child_name))
                {
                    if (!right_child.isEmpty())
                        return false;
                }
            }
        }

        return true;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }
};


AccessRights::AccessRights() = default;
AccessRights::~AccessRights() = default;
AccessRights::AccessRights(AccessRights && src) = default;
AccessRights & AccessRights::operator =(AccessRights && src) = default;


AccessRights::AccessRights(const AccessRights & src)
{
    *this = src;
}


AccessRights & AccessRights::operator =(const AccessRights & src)
{
    if (src.root)
        root = std::make_unique<Node>(*src.root);
    else
        root = nullptr;
    return *this;
}


AccessRights::AccessRights(const AccessType & access)
{
    grant(access);
}


bool AccessRights::isEmpty() const
{
    return !root || root->isEmpty();
}


void AccessRights::clear()
{
    root = nullptr;
}


template <typename... Args>
void AccessRights::grantImpl(const AccessType & access, const Args &... args)
{
    if constexpr (sizeof...(args) == 1)
    {
        if (access != AccessType::ALL)
            access.checkGrantableOnDatabaseLevel();
    }
    else if constexpr (sizeof...(args) == 2)
    {
        if (access != AccessType::ALL)
            access.checkGrantableOnTableLevel();
    }
    else if constexpr (sizeof...(args) == 3)
    {
        if (access != AccessType::ALL)
            access.checkGrantableOnColumnLevel();
    }

    if (!root)
        root = std::make_unique<Node>();
    root->grant(access, args...);
}

void AccessRights::grant(const AccessType & access) { grantImpl(access); }
void AccessRights::grant(const AccessType & access, const std::string_view & database) { grantImpl(access, database); }
void AccessRights::grant(const AccessType & access, const Strings & databases) { grantImpl(access, databases); }
void AccessRights::grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) { grantImpl(access, database, table_or_dictionary); }
void AccessRights::grant(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) { grantImpl(access, database, tables_or_dictionaries); }
void AccessRights::grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) { grantImpl(access, database, table_or_dictionary, column_or_attribute); }
void AccessRights::grant(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) { grantImpl(access, database, table_or_dictionary, columns_or_attributes); }


template <typename... Args>
void AccessRights::revokeImpl(const AccessType & access, const Args &... args)
{
    if (!root)
        return;
    root->revoke<RevokeMode::NORMAL>(access, args...);
}

void AccessRights::revoke(const AccessType & access) { revokeImpl(access); }
void AccessRights::revoke(const AccessType & access, const std::string_view & database) { revokeImpl(access, database); }
void AccessRights::revoke(const AccessType & access, const Strings & databases) { revokeImpl(access, databases); }
void AccessRights::revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) { revokeImpl(access, database, table_or_dictionary); }
void AccessRights::revoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) { revokeImpl(access, database, tables_or_dictionaries); }
void AccessRights::revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) { revokeImpl(access, database, table_or_dictionary, column_or_attribute); }
void AccessRights::revoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) { revokeImpl(access, database, table_or_dictionary, columns_or_attributes); }


template <typename... Args>
void AccessRights::partialRevokeImpl(const AccessType & access, const Args &... args)
{
    if (!root)
        return;
    root->revoke<RevokeMode::PARTIAL>(access, args...);
}

void AccessRights::partialRevoke(const AccessType & access) { partialRevokeImpl(access); }
void AccessRights::partialRevoke(const AccessType & access, const std::string_view & database) { partialRevokeImpl(access, database); }
void AccessRights::partialRevoke(const AccessType & access, const Strings & databases) { partialRevokeImpl(access, databases); }
void AccessRights::partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) { partialRevokeImpl(access, database, table_or_dictionary); }
void AccessRights::partialRevoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) { partialRevokeImpl(access, database, tables_or_dictionaries); }
void AccessRights::partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) { partialRevokeImpl(access, database, table_or_dictionary, column_or_attribute); }
void AccessRights::partialRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) { partialRevokeImpl(access, database, table_or_dictionary, columns_or_attributes); }


template <typename... Args>
void AccessRights::fullRevokeImpl(const AccessType & access, const Args &... args)
{
    if (!root)
        return;
    if constexpr (sizeof...(args) == 0)
    {
        if (access == AccessType::ALL)
        {
            clear();
            return;
        }
    }
    root->revoke<RevokeMode::FULL>(access, args...);
}

void AccessRights::fullRevoke(const AccessType & access) { fullRevokeImpl(access); }
void AccessRights::fullRevoke(const AccessType & access, const std::string_view & database) { fullRevokeImpl(access, database); }
void AccessRights::fullRevoke(const AccessType & access, const Strings & databases) { fullRevokeImpl(access, databases); }
void AccessRights::fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) { fullRevokeImpl(access, database, table_or_dictionary); }
void AccessRights::fullRevoke(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) { fullRevokeImpl(access, database, tables_or_dictionaries); }
void AccessRights::fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) { fullRevokeImpl(access, database, table_or_dictionary, column_or_attribute); }
void AccessRights::fullRevoke(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) { fullRevokeImpl(access, database, table_or_dictionary, columns_or_attributes); }


std::vector<AccessRights::Info> AccessRights::getInfos() const
{
    if (!root)
        return {};
    std::vector<Info> infos;
    if (root->explicit_grants)
        infos.push_back({root->explicit_grants, {}, {}, {}, Kind::GRANT});
    if (root->children)
    {
        for (const auto & [db_name, db_node] : *root->children)
        {
            if (db_node.partial_revokes)
                infos.push_back({db_node.partial_revokes, String{db_name}, {}, {}, Kind::PARTIAL_REVOKE});
            if (db_node.explicit_grants)
                infos.push_back({db_node.explicit_grants, String{db_name}, {}, {}, Kind::GRANT});
            if (db_node.children)
            {
                for (const auto & [table_name, table_node] : *db_node.children)
                {
                    if (table_node.partial_revokes)
                        infos.push_back({table_node.partial_revokes, String{db_name}, String{table_name}, {}, Kind::PARTIAL_REVOKE});
                    if (table_node.explicit_grants)
                        infos.push_back({table_node.explicit_grants, String{db_name}, String{table_name}, {}, Kind::GRANT});
                    if (table_node.children)
                    {
                        for (const auto & [column_name, column_node] : *table_node.children)
                        {
                            if (column_node.partial_revokes)
                                infos.push_back({column_node.partial_revokes, String{db_name}, String{table_name}, String{column_name}, Kind::PARTIAL_REVOKE});
                            if (column_node.explicit_grants)
                                infos.push_back({column_node.explicit_grants, String{db_name}, String{table_name}, String{column_name}, Kind::GRANT});
                        }
                    }
                }
            }
        }
    }
    return infos;
}


String AccessRights::Info::toString() const
{
    if (!column_or_attribute.empty())
        return grantToStringImpl(kind, access, database, table_or_dictionary, column_or_attribute);
    else if (!table_or_dictionary.empty())
        return grantToStringImpl(kind, access, database, table_or_dictionary);
    else if (!database.empty())
        return grantToStringImpl(kind, access, database);
    else
        return grantToStringImpl(kind, access);
}


template <typename... Args>
bool AccessRights::isGrantedImpl(const AccessType & access, const Args &... args) const
{
    if (!root)
        return access.isEmpty();
    return root->getAccess(args...).contains(access);
}

bool AccessRights::isGranted(const AccessType & access) const { return isGrantedImpl(access); }
bool AccessRights::isGranted(const AccessType & access, const std::string_view & database) const { return isGrantedImpl(access, database); }
bool AccessRights::isGranted(const AccessType & access, const Strings & databases) const { return isGrantedImpl(access, databases); }
bool AccessRights::isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) const { return isGrantedImpl(access, database, table_or_dictionary); }
bool AccessRights::isGranted(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) const { return isGrantedImpl(access, database, tables_or_dictionaries); }
bool AccessRights::isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) const { return isGrantedImpl(access, database, table_or_dictionary, column_or_attribute); }
bool AccessRights::isGranted(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) const { return isGrantedImpl(access, database, table_or_dictionary, columns_or_attributes); }


template <typename... Args>
AccessType AccessRights::getAccessImpl(const Args &... args) const
{
    if (!root)
        return {};
    return root->getAccess(args...);
}

AccessType AccessRights::getAccess() const { return getAccessImpl(); }
AccessType AccessRights::getAccess(const std::string_view & database) const { return getAccessImpl(database); }
AccessType AccessRights::getAccess(const Strings & databases) const { return getAccessImpl(databases); }
AccessType AccessRights::getAccess(const std::string_view & database, const std::string_view & table_or_dictionary) const { return getAccessImpl(database, table_or_dictionary); }
AccessType AccessRights::getAccess(const std::string_view & database, const Strings & tables_or_dictionaries) const { return getAccessImpl(database, tables_or_dictionaries); }
AccessType AccessRights::getAccess(const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) const { return getAccessImpl(database, table_or_dictionary, column_or_attribute); }
AccessType AccessRights::getAccess(const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) const { return getAccessImpl(database, table_or_dictionary, columns_or_attributes); }


bool operator ==(const AccessRights & left, const AccessRights & right)
{
    if (!left.root)
        return right.isEmpty();
    else if (!right.root)
        return left.isEmpty();
    else
        return *left.root == *right.root;
}


void AccessRights::merge(const AccessRights & other)
{
    if (!root)
        *this = other;
    else if (other.root)
        root->merge(*other.root);
}


String AccessRights::grantToString(const AccessType & access) { return grantToStringImpl(access); }
String AccessRights::grantToString(const AccessType & access, const std::string_view & database) { return grantToStringImpl(access, database); }
String AccessRights::grantToString(const AccessType & access, const Strings & databases) { return grantToStringImpl(access, databases); }
String AccessRights::grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary) { return grantToStringImpl(access, database, table_or_dictionary); }
String AccessRights::grantToString(const AccessType & access, const std::string_view & database, const Strings & tables_or_dictionaries) { return grantToStringImpl(access, database, tables_or_dictionaries); }
String AccessRights::grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const std::string_view & column_or_attribute) { return grantToStringImpl(access, database, table_or_dictionary, column_or_attribute); }
String AccessRights::grantToString(const AccessType & access, const std::string_view & database, const std::string_view & table_or_dictionary, const Strings & columns_or_attributes) { return grantToStringImpl(access, database, table_or_dictionary, columns_or_attributes); }

}
