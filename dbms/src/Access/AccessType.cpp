#include <Access/AccessType.h>
#include <Common/Exception.h>
#include <ext/movables.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <unordered_map>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ACCESS_TYPE;
    extern const int INVALID_GRANT;
}

namespace
{
    enum GrantableOn
    {
        ON_DATABASE = 0x01,
        ON_TABLE = 0x02,
        ON_VIEW = ON_TABLE,
        ON_DICTIONARY = 0x04,
        ON_COLUMN = 0x08,
        ON_ATTRIBUTE = 0x10,
    };

    static constexpr size_t NUM_FLAGS = 64;
    using Flags = std::bitset<NUM_FLAGS>;

    struct FlagsToKeywordsTree
    {
        struct Node;
        using NodePtr = std::unique_ptr<Node>;
        using Nodes = std::vector<NodePtr>;

        struct Node
        {
            std::string_view keyword;
            std::vector<std::string_view> aliases;
            Flags flags;
            bool is_single_flag = false;
            std::optional<size_t> main_flag;
            int grantable_on = 0;
            Nodes children;

            Node(std::string_view keyword_, size_t flag_, int grantable_on_ = 0)
                : keyword(keyword_), grantable_on(grantable_on_)
            {
                flags.set(flag_);
                main_flag = flag_;
                is_single_flag = true;
            }

            Node(std::string_view keyword_, Nodes children_)
                : keyword(keyword_), children(std::move(children_))
            {
                for (const auto & child : children)
                {
                    flags |= child->flags;
                    grantable_on |= child->grantable_on;
                }

                if ((children.size() == 1) && children[0]->is_single_flag)
                {
                    is_single_flag = true;
                    main_flag = children[0]->main_flag;
                }
            }

            Node(std::string_view keyword_, size_t flag_, Nodes children_)
                : Node(keyword_, std::move(children_))
            {
                main_flag = flag_;
                is_single_flag = children.empty();
            }
        };

        static const FlagsToKeywordsTree & instance()
        {
            static const FlagsToKeywordsTree the_instance;
            return the_instance;
        }

        const Node * getRoot() const { return root.get(); }

    private:
        FlagsToKeywordsTree()
        {
            size_t next_flag = 0;
            Nodes all_nodes;

            auto select = std::make_unique<Node>("SELECT", next_flag++, ON_COLUMN);
            auto insert = std::make_unique<Node>("INSERT", next_flag++, ON_COLUMN);
            auto update = std::make_unique<Node>("UPDATE", next_flag++, ON_COLUMN);
            auto delet = std::make_unique<Node>("DELETE", next_flag++, ON_COLUMN);
            boost::range::push_back(all_nodes, ext::movables{std::move(select), std::move(insert), std::move(update), std::move(delet)});

            auto all = std::make_unique<Node>("ALL", next_flag++, std::move(all_nodes));
            all->aliases.push_back("ALL PRIVILEGES");
            root = std::move(all);
        }

        std::unique_ptr<Node> root;
    };


    Strings flagsToKeywords(const Flags & flags)
    {
        using Node = FlagsToKeywordsTree::Node;

        static constexpr const Node * (*find_node)(const Flags &, bool, const Node *)
            = [](const Flags & flags_, bool require_exact_match, const Node * node) -> const Node *
        {
            if (node->is_single_flag)
            {
                if (flags_.test(*node->main_flag))
                    return node;
            }
            else
            {
                if (node->main_flag && !require_exact_match)
                {
                    if (flags_.test(*node->main_flag))
                        return node;
                }
                if ((flags_ & node->flags) == node->flags)
                    return node;
            }
            for (const auto & child : node->children)
            {
                if (const auto * x = find_node(flags_, require_exact_match, child.get()))
                    return x;
            }
            return nullptr;
        };

        Strings keywords;
        Flags current_flags = flags;
        const auto & tree = FlagsToKeywordsTree::instance();
        while (const Node * include_node = find_node(current_flags, false, tree.getRoot()))
        {
            String keyword(include_node->keyword);
            Flags exclude_flags = include_node->flags & ~current_flags;
            current_flags &= ~include_node->flags;

            if (exclude_flags.any())
            {
                size_t num_excludes = 0;
                for (const auto & child : include_node->children)
                {
                    while (const Node * exclude_node = find_node(exclude_flags, true, child.get()))
                    {
                        keyword += (num_excludes++ ? " AND " : " EXCEPT ") + String(exclude_node->keyword);
                        exclude_flags &= ~exclude_node->flags;
                    }
                }
            }

            keywords.push_back(std::move(keyword));
        }

        if (keywords.empty())
            keywords.push_back("USAGE");

        return keywords;
    }


    String flagsToString(const Flags & flags)
    {
        return boost::join(flagsToKeywords(flags), ", ");
    }


    using KeywordToFlagsMap = std::unordered_map<std::string_view, Flags>;

    const KeywordToFlagsMap & getKeywordToFlagsMap()
    {
        static const KeywordToFlagsMap the_map = []
        {
            KeywordToFlagsMap map;
            using Node = FlagsToKeywordsTree::Node;

            std::function<void(const Node *)> fill_map;
            fill_map = [&map, &fill_map](const Node * node)
            {
                map[node->keyword] = node->flags;
                for (const auto & alias : node->aliases)
                    map[alias] = node->flags;
                for (const auto & child : node->children)
                    fill_map(child.get());
            };

            fill_map(FlagsToKeywordsTree::instance().getRoot());
            map["USAGE"] = Flags{};
            return map;
        }();
        return the_map;
    }


    Flags parseKeyword(const std::string_view & keyword, const KeywordToFlagsMap & keyword_to_info = getKeywordToFlagsMap())
    {
        auto parse_keyword_impl = [&keyword_to_info](const std::string_view & keyword_, bool add_flags, Flags & out_flags)
        {
            auto it = keyword_to_info.find(keyword_);
            if (it == keyword_to_info.end())
                throw Exception("Unknown access type: " + String(keyword_), ErrorCodes::UNKNOWN_ACCESS_TYPE);
            if (add_flags)
                out_flags |= it->second;
            else
                out_flags &= it->second;
        };

        Flags flags;
        size_t pos = 0;
        size_t num_excludes = 0;
        while (true)
        {
            std::string_view separator = num_excludes ? " AND " : " EXCEPT ";
            size_t next_pos = keyword.find(separator, pos);
            if (next_pos == std::string_view::npos)
            {
                parse_keyword_impl(keyword.substr(pos), !num_excludes, flags);
                break;
            }

            parse_keyword_impl(keyword.substr(pos, next_pos - pos), !num_excludes, flags);
            ++num_excludes;
            pos = next_pos + separator.length();
        }

        return flags;
    }


    using TypeToFlagsMapping = std::vector<Flags>;

    const TypeToFlagsMapping & getTypeToFlagsMapping()
    {
        static const TypeToFlagsMapping the_mapping = []
        {
            TypeToFlagsMapping mapping;
            const auto & keyword_to_flags = getKeywordToFlagsMap();
            String temp_string;

            auto add_element_to_mapping = [&mapping, &keyword_to_flags, &temp_string](AccessType::Type type, std::string_view keyword)
            {
                temp_string = String(keyword);
                boost::algorithm::replace_all(temp_string, "_", " ");
                mapping.resize(std::max(mapping.size(), static_cast<size_t>(type) + 1));
                mapping[static_cast<size_t>(type)] = keyword_to_flags.at(temp_string);
            };

            add_element_to_mapping(AccessType::Type::NONE, "USAGE");

#define ADD_ELEMENT_TO_MAPPING(type) add_element_to_mapping(AccessType::Type::type, #type)

            ADD_ELEMENT_TO_MAPPING(SELECT);
            ADD_ELEMENT_TO_MAPPING(INSERT);
            ADD_ELEMENT_TO_MAPPING(UPDATE);
            ADD_ELEMENT_TO_MAPPING(DELETE);

            ADD_ELEMENT_TO_MAPPING(ALL);

#undef ADD_ELEMENT_TO_MAPPING
            return mapping;
        }();
        return the_mapping;
    };


    template <int grantable_on>
    const Flags & getAllGrantable()
    {
        static const Flags the_value = []
        {
            using Node = FlagsToKeywordsTree::Node;

            static constexpr void(*collect_flags)(Flags &, const Node *)
                = [](Flags & res, const Node * node)
            {
                if (node->grantable_on & grantable_on)
                    res |= node->flags;
                for (const auto & child : node->children)
                    collect_flags(res, child.get());
            };

            Flags value;
            collect_flags(value, FlagsToKeywordsTree::instance().getRoot());
            return value;
        }();
        return the_value;
    }
}


AccessType::AccessType(Type type)
    : flags(getTypeToFlagsMapping()[static_cast<size_t>(type)])
{
}


AccessType::AccessType(const std::string_view & keyword)
    : flags(parseKeyword(keyword))
{
}


AccessType::AccessType(const std::vector<std::string_view> & keywords)
{
    const auto & keyword_to_flags = getKeywordToFlagsMap();
    for (const auto & keyword : keywords)
        flags |= parseKeyword(keyword, keyword_to_flags);
}


AccessType::AccessType(const Strings & keywords)
{
    const auto & keyword_to_flags = getKeywordToFlagsMap();
    for (const auto & keyword : keywords)
        flags |= parseKeyword(keyword, keyword_to_flags);
}


Strings AccessType::toKeywords() const
{
    return flagsToKeywords(flags);
}


String AccessType::toString() const
{
    return flagsToString(flags);
}


void AccessType::checkGrantableOnDatabaseLevel() const
{
    static const Flags not_grantable = ~getAllGrantable<ON_DATABASE | ON_TABLE | ON_VIEW | ON_DICTIONARY | ON_COLUMN | ON_ATTRIBUTE>();
    Flags current_not_grantable = flags & not_grantable;
    if (current_not_grantable.any())
        throw Exception("Access types " + flagsToString(current_not_grantable) + " cannot be granted on database level", ErrorCodes::INVALID_GRANT);
}

void AccessType::checkGrantableOnTableLevel() const
{
    static const Flags not_grantable = ~getAllGrantable<ON_TABLE | ON_VIEW | ON_DICTIONARY | ON_COLUMN | ON_ATTRIBUTE>();
    Flags current_not_grantable = flags & not_grantable;
    if (current_not_grantable.any())
        throw Exception("Access types " + flagsToString(current_not_grantable) + " cannot be granted on table level", ErrorCodes::INVALID_GRANT);
}

void AccessType::checkGrantableOnColumnLevel() const
{
    static const Flags not_grantable = ~getAllGrantable<ON_COLUMN | ON_ATTRIBUTE>();
    Flags current_not_grantable = flags & not_grantable;
    if (current_not_grantable.any())
        throw Exception("Access types " + flagsToString(current_not_grantable) + " cannot be granted on column level", ErrorCodes::INVALID_GRANT);
}

}
