#include <Access/AccessRights.h>
#include <Common/Exception.h>
#include <boost/range/adaptor/map.hpp>
#include <unordered_map>


namespace DB
{
namespace ErrorCodes
{
    extern const int INVALID_GRANT;
    extern const int LOGICAL_ERROR;
}


namespace
{
    enum Level
    {
        GLOBAL_LEVEL = 0,
        DATABASE_LEVEL = 1,
        TABLE_LEVEL = 2,
        COLUMN_LEVEL = 3,
    };

    enum RevokeMode
    {
        NORMAL_REVOKE_MODE,  /// for AccessRights::revoke()
        PARTIAL_REVOKE_MODE, /// for AccessRights::partialRevoke()
        FULL_REVOKE_MODE,    /// for AccessRights::fullRevoke()
    };

    std::string_view checkCurrentDatabase(const std::string_view & current_database)
    {
        if (current_database.empty())
            throw Exception("Current database is not set", ErrorCodes::LOGICAL_ERROR);
        return current_database;
    }
}


struct AccessRights::Node
{
public:
    std::shared_ptr<const String> node_name;
    AccessFlags explicit_grants;
    AccessFlags inherited_access;
    AccessFlags partial_revokes;
    AccessFlags access; /// access == (inherited_access - partial_revokes) | explicit_grants
    AccessFlags access_with_children; /// access_with_children == access & child[0].access_with_children & ... & child[N].access_with_children
    Level level = GLOBAL_LEVEL;
    std::unique_ptr<std::unordered_map<std::string_view, Node>> children;

    Node() = default;
    Node(Node && src) = default;
    Node & operator =(Node && src) = default;

    Node(const Node & src) { *this = src; }

    Node & operator =(const Node & src)
    {
        node_name = src.node_name;
        access = src.access;
        access_with_children = src.access_with_children;
        explicit_grants = src.explicit_grants;
        inherited_access = src.inherited_access;
        partial_revokes = src.partial_revokes;
        level = src.level;
        if (src.children)
            children = std::make_unique<std::unordered_map<std::string_view, Node>>(*src.children);
        else
            children = nullptr;
        return *this;
    }

    void grant(const AccessFlags & access_to_grant)
    {
        explicit_grants |= access_to_grant - partial_revokes;
        partial_revokes -= access_to_grant;
        recalculateAccess();
    }

    template <typename ... Args>
    void grant(const AccessFlags & access_to_grant, const std::string_view & name, const Args &... subnames)
    {
        auto & child = getChild(name);
        child.grant(access_to_grant, subnames...);
        eraseChildIfEmpty(child);
        recalculateOnlyAccessWithChildren();
    }

    template <typename StringT>
    void grant(const AccessFlags & access_to_grant, const std::vector<StringT> & names)
    {
        for (const auto & name : names)
        {
            auto & child = getChild(name);
            child.grant(access_to_grant);
            eraseChildIfEmpty(child);
        }
        recalculateOnlyAccessWithChildren();
    }

    template <int mode>
    void revoke(const AccessFlags & access_to_revoke)
    {
        if constexpr (mode == NORMAL_REVOKE_MODE)
        {
            explicit_grants -= access_to_revoke;
        }
        else if constexpr (mode == PARTIAL_REVOKE_MODE)
        {
            partial_revokes |= access_to_revoke - explicit_grants;
            explicit_grants -= access_to_revoke;
        }
        else /// mode == FULL_REVOKE_MODE
        {
            fullRevokeRecalculateExplicitGrantsAndPartialRevokes(access_to_revoke);
        }
        recalculateAccess();
    }

    template <int mode, typename... Args>
    void revoke(const AccessFlags & access_to_revoke, const std::string_view & name, const Args &... subnames)
    {
        Node * child;
        if (mode == NORMAL_REVOKE_MODE)
        {
            if (!(child = tryGetChild(name)))
                return;
        }
        else
            child = &getChild(name);

        child->revoke<mode>(access_to_revoke, subnames...);
        eraseChildIfEmpty(*child);
        recalculateOnlyAccessWithChildren();
    }

    template <int mode, typename StringT>
    void revoke(const AccessFlags & access_to_revoke, const std::vector<StringT> & names)
    {
        Node * child;
        for (const auto & name : names)
        {
            if (mode == NORMAL_REVOKE_MODE)
            {
                if (!(child = tryGetChild(name)))
                    continue;
            }
            else
                child = &getChild(name);

            child->revoke<mode>(access_to_revoke);
            eraseChildIfEmpty(*child);
        }
        recalculateOnlyAccessWithChildren();
    }

    const AccessFlags & getAccess() const { return access_with_children; }

    template <typename... Args>
    AccessFlags getAccess(const std::string_view & name, const Args &... subnames) const
    {
        const Node * child = tryGetChild(name);
        return child ? child->getAccess(subnames...) : access;
    }

    template <typename StringT>
    AccessFlags getAccess(const std::vector<StringT> & names) const
    {
        AccessFlags r = AccessType::ALL;
        for (const auto & name : names)
            r &= getAccess(name);
        return r;
    }

    friend bool operator ==(const Node & left, const Node & right)
    {
        if ((left.explicit_grants != right.explicit_grants) || (left.partial_revokes != right.partial_revokes))
            return false;

        if (!left.children)
            return !right.children;

        if (!right.children)
            return false;
        return *left.children == *right.children;
    }

    friend bool operator!=(const Node & left, const Node & right) { return !(left == right); }

    bool isEmpty() const
    {
        return !explicit_grants && !partial_revokes && !children;
    }

    void merge(const Node & other)
    {
        mergeAccess(other);
        recalculateGrantsAndPartialRevokesAndAccessWithChildren();
    }

private:
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
        new_child.level = static_cast<Level>(level + 1);
        return new_child;
    }

    void eraseChildIfEmpty(const Node & child)
    {
        if (!child.isEmpty())
            return;
        children->erase(*child.node_name);
        if (children->empty())
            children.reset();
    }

    void recalculateAccess()
    {
        partial_revokes &= inherited_access;
        access = (inherited_access - partial_revokes) | explicit_grants;
        if (access)
            access |= AccessType::SHOW; /// The SHOW access type is granted implicitly.
        access_with_children = access;

        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
            {
                child.inherited_access = access;
                child.recalculateAccess();
                access_with_children &= child.access_with_children;
            }
        }
    }

    void recalculateOnlyAccessWithChildren()
    {
        access_with_children = access;
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                access_with_children &= child.access_with_children;
        }
    }

    void fullRevokeRecalculateExplicitGrantsAndPartialRevokes(const AccessFlags & access_to_revoke)
    {
        explicit_grants -= access_to_revoke;
        partial_revokes |= access_to_revoke;
        if (children)
        {
            for (auto & child : *children | boost::adaptors::map_values)
                child.fullRevokeRecalculateExplicitGrantsAndPartialRevokes(access_to_revoke);
        }
    }

    void mergeAccess(const Node & rhs)
    {
        if (rhs.children)
        {
            for (const auto & [rhs_childname, rhs_child] : *rhs.children)
                getChild(rhs_childname).mergeAccess(rhs_child);
        }
        access |= rhs.access;
        if (children)
        {
            for (auto & [lhs_childname, lhs_child] : *children)
            {
                if (!rhs.tryGetChild(lhs_childname))
                    lhs_child.access |= rhs.access;
            }
        }
    }

    void recalculateGrantsAndPartialRevokesAndAccessWithChildren()
    {
        explicit_grants = access - inherited_access;
        partial_revokes = inherited_access - access;
        access_with_children = access;
        if (children)
        {
            for (auto it = children->begin(); it != children->end();)
            {
                auto & child = it->second;
                child.recalculateGrantsAndPartialRevokesAndAccessWithChildren();
                if (child.isEmpty())
                    it = children->erase(it);
                else
                {
                    access_with_children &= child.access_with_children;
                    ++it;
                }
            }
            if (children->empty())
                children.reset();
        }
    }
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


AccessRights::AccessRights(const AccessFlags & access)
{
    grant(access);
}


bool AccessRights::isEmpty() const
{
    return !root;
}


void AccessRights::clear()
{
    root = nullptr;
}


template <typename... Args>
void AccessRights::grantImpl(const AccessFlags & access, const Args &... args)
{
    AccessFlags grantable;
    if constexpr (sizeof...(args) == 0)
    {
        /// Everything can be granted on the global level.
        grantable = access;
    }
    else if constexpr (sizeof...(args) == 1)
    {
        grantable = access & AccessFlags::allGrantableOnDatabaseLevel();
        if (!grantable && access)
            throw Exception(access.toString() + " cannot be granted on the database level", ErrorCodes::INVALID_GRANT);
    }
    else if constexpr (sizeof...(args) == 2)
    {
        grantable = access & AccessFlags::allGrantableOnTableLevel();
        if (!grantable && access)
            throw Exception(access.toString() + " cannot be granted on the table level", ErrorCodes::INVALID_GRANT);
    }
    else if constexpr (sizeof...(args) == 3)
    {
        grantable = access & AccessFlags::allGrantableOnColumnLevel();
        if (!grantable && access)
            throw Exception(access.toString() + " cannot be granted on the column level", ErrorCodes::INVALID_GRANT);
    }

    if (!root)
        root = std::make_unique<Node>();
    root->grant(grantable, args...);
    if (root->isEmpty())
        root.reset();
}

void AccessRights::grantImpl(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        grantImpl(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.current_database)
            grantImpl(element.access_flags, checkCurrentDatabase(current_database));
        else
            grantImpl(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.current_database)
            grantImpl(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            grantImpl(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.current_database)
            grantImpl(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            grantImpl(element.access_flags, element.database, element.table, element.columns);
    }
}

void AccessRights::grantImpl(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        grantImpl(element, current_database);
}

void AccessRights::grant(const AccessFlags & access) { grantImpl(access); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database) { grantImpl(access, database); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { grantImpl(access, database, table); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { grantImpl(access, database, table, column); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { grantImpl(access, database, table, columns); }
void AccessRights::grant(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { grantImpl(access, database, table, columns); }
void AccessRights::grant(const AccessRightsElement & element, std::string_view current_database) { grantImpl(element, current_database); }
void AccessRights::grant(const AccessRightsElements & elements, std::string_view current_database) { grantImpl(elements, current_database); }

template <int mode, typename... Args>
void AccessRights::revokeImpl(const AccessFlags & access, const Args &... args)
{
    if (!root)
        return;
    root->revoke<mode>(access, args...);
    if (root->isEmpty())
        root.reset();
}

template <int mode>
void AccessRights::revokeImpl(const AccessRightsElement & element, std::string_view current_database)
{
    if (element.any_database)
    {
        revokeImpl<mode>(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.current_database)
            revokeImpl<mode>(element.access_flags, checkCurrentDatabase(current_database));
        else
            revokeImpl<mode>(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.current_database)
            revokeImpl<mode>(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            revokeImpl<mode>(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.current_database)
            revokeImpl<mode>(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            revokeImpl<mode>(element.access_flags, element.database, element.table, element.columns);
    }
}

template <int mode>
void AccessRights::revokeImpl(const AccessRightsElements & elements, std::string_view current_database)
{
    for (const auto & element : elements)
        revokeImpl<mode>(element, current_database);
}

void AccessRights::revoke(const AccessFlags & access) { revokeImpl<NORMAL_REVOKE_MODE>(access); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<NORMAL_REVOKE_MODE>(access, database); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::revoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<NORMAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::revoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<NORMAL_REVOKE_MODE>(element, current_database); }
void AccessRights::revoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<NORMAL_REVOKE_MODE>(elements, current_database); }

void AccessRights::partialRevoke(const AccessFlags & access) { revokeImpl<PARTIAL_REVOKE_MODE>(access); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::partialRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<PARTIAL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::partialRevoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<PARTIAL_REVOKE_MODE>(element, current_database); }
void AccessRights::partialRevoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<PARTIAL_REVOKE_MODE>(elements, current_database); }

void AccessRights::fullRevoke(const AccessFlags & access) { revokeImpl<FULL_REVOKE_MODE>(access); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database) { revokeImpl<FULL_REVOKE_MODE>(access, database); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table) { revokeImpl<FULL_REVOKE_MODE>(access, database, table); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, column); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::fullRevoke(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) { revokeImpl<FULL_REVOKE_MODE>(access, database, table, columns); }
void AccessRights::fullRevoke(const AccessRightsElement & element, std::string_view current_database) { revokeImpl<FULL_REVOKE_MODE>(element, current_database); }
void AccessRights::fullRevoke(const AccessRightsElements & elements, std::string_view current_database) { revokeImpl<FULL_REVOKE_MODE>(elements, current_database); }


AccessRights::Elements AccessRights::getElements() const
{
    if (!root)
        return {};
    Elements res;
    if (root->explicit_grants)
        res.grants.push_back({root->explicit_grants});
    if (root->children)
    {
        for (const auto & [db_name, db_node] : *root->children)
        {
            if (db_node.partial_revokes)
                res.partial_revokes.push_back({db_node.partial_revokes, db_name});
            if (db_node.explicit_grants)
                res.grants.push_back({db_node.explicit_grants, db_name});
            if (db_node.children)
            {
                for (const auto & [table_name, table_node] : *db_node.children)
                {
                    if (table_node.partial_revokes)
                        res.partial_revokes.push_back({table_node.partial_revokes, db_name, table_name});
                    if (table_node.explicit_grants)
                        res.grants.push_back({table_node.explicit_grants, db_name, table_name});
                    if (table_node.children)
                    {
                        for (const auto & [column_name, column_node] : *table_node.children)
                        {
                            if (column_node.partial_revokes)
                                res.partial_revokes.push_back({column_node.partial_revokes, db_name, table_name, column_name});
                            if (column_node.explicit_grants)
                                res.grants.push_back({column_node.explicit_grants, db_name, table_name, column_name});
                        }
                    }
                }
            }
        }
    }
    return res;
}


String AccessRights::toString() const
{
    auto elements = getElements();
    String res;
    if (elements.grants.empty())
    {
        res += "GRANT ";
        res += elements.grants.toString();
    }
    if (elements.partial_revokes.empty())
    {
        if (!res.empty())
            res += ", ";
        res += "REVOKE ";
        res += elements.partial_revokes.toString();
    }
    if (res.empty())
        res = "GRANT USAGE ON *.*";
    return res;
}


template <typename... Args>
bool AccessRights::isGrantedImpl(const AccessFlags & access, const Args &... args) const
{
    if (!root)
        return access.isEmpty();
    return root->getAccess(args...).contains(access);
}

bool AccessRights::isGrantedImpl(const AccessRightsElement & element, std::string_view current_database) const
{
    if (element.any_database)
    {
        return isGrantedImpl(element.access_flags);
    }
    else if (element.any_table)
    {
        if (element.current_database)
            return isGrantedImpl(element.access_flags, checkCurrentDatabase(current_database));
        else
            return isGrantedImpl(element.access_flags, element.database);
    }
    else if (element.any_column)
    {
        if (element.current_database)
            return isGrantedImpl(element.access_flags, checkCurrentDatabase(current_database), element.table);
        else
            return isGrantedImpl(element.access_flags, element.database, element.table);
    }
    else
    {
        if (element.current_database)
            return isGrantedImpl(element.access_flags, checkCurrentDatabase(current_database), element.table, element.columns);
        else
            return isGrantedImpl(element.access_flags, element.database, element.table, element.columns);
    }
}

bool AccessRights::isGrantedImpl(const AccessRightsElements & elements, std::string_view current_database) const
{
    for (const auto & element : elements)
        if (!isGrantedImpl(element, current_database))
            return false;
    return true;
}

bool AccessRights::isGranted(const AccessFlags & access) const { return isGrantedImpl(access); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database) const { return isGrantedImpl(access, database); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table) const { return isGrantedImpl(access, database, table); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return isGrantedImpl(access, database, table, column); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return isGrantedImpl(access, database, table, columns); }
bool AccessRights::isGranted(const AccessFlags & access, const std::string_view & database, const std::string_view & table, const Strings & columns) const { return isGrantedImpl(access, database, table, columns); }
bool AccessRights::isGranted(const AccessRightsElement & element, std::string_view current_database) const { return isGrantedImpl(element, current_database); }
bool AccessRights::isGranted(const AccessRightsElements & elements, std::string_view current_database) const { return isGrantedImpl(elements, current_database); }


template <typename... Args>
AccessFlags AccessRights::getAccessImpl(const Args &... args) const
{
    if (!root)
        return {};
    return root->getAccess(args...);
}

AccessFlags AccessRights::getAccess() const { return getAccessImpl(); }
AccessFlags AccessRights::getAccess(const std::string_view & database) const { return getAccessImpl(database); }
AccessFlags AccessRights::getAccess(const std::string_view & database, const std::string_view & table) const { return getAccessImpl(database, table); }
AccessFlags AccessRights::getAccess(const std::string_view & database, const std::string_view & table, const std::string_view & column) const { return getAccessImpl(database, table, column); }
AccessFlags AccessRights::getAccess(const std::string_view & database, const std::string_view & table, const std::vector<std::string_view> & columns) const { return getAccessImpl(database, table, columns); }
AccessFlags AccessRights::getAccess(const std::string_view & database, const std::string_view & table, const Strings & columns) const { return getAccessImpl(database, table, columns); }


bool operator ==(const AccessRights & left, const AccessRights & right)
{
    if (!left.root)
        return !right.root;
    if (!right.root)
        return false;
    return *left.root == *right.root;
}


void AccessRights::merge(const AccessRights & other)
{
    if (!root)
        *this = other;
    else if (other.root)
        root->merge(*other.root);
}

}
