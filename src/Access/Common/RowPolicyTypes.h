#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <boost/algorithm/string/case_conv.hpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

struct RowPolicyName
{
    String short_name;
    String database;
    String table_name;

    bool empty() const { return short_name.empty(); }
    String toString() const;
    auto toTuple() const { return std::tie(short_name, database, table_name); }
    friend bool operator ==(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() == right.toTuple(); }
    friend bool operator !=(const RowPolicyName & left, const RowPolicyName & right) { return left.toTuple() != right.toTuple(); }
};

enum class RowPolicyConditionType
{
    SELECT_FILTER,

#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
    INSERT_CHECK,
    UPDATE_FILTER,
    UPDATE_CHECK,
    DELETE_FILTER,
#endif

    MAX
};

struct RowPolicyConditionTypeInfo
{
    const char * const raw_name;
    const String name;    /// Lowercased with underscores, e.g. "select_filter".
    const String command; /// Uppercased without last word, e.g. "SELECT".
    const bool is_check;  /// E.g. false for SELECT_FILTER.
    static const RowPolicyConditionTypeInfo & get(RowPolicyConditionType type);
};

inline String RowPolicyName::toString() const
{
    String name;
    name.reserve(database.length() + table_name.length() + short_name.length() + 6);
    name += backQuoteIfNeed(short_name);
    name += " ON ";
    if (!database.empty())
    {
        name += backQuoteIfNeed(database);
        name += '.';
    }
    name += backQuoteIfNeed(table_name);
    return name;
}

inline const RowPolicyConditionTypeInfo & RowPolicyConditionTypeInfo::get(RowPolicyConditionType type_)
{
    static constexpr auto make_info = [](const char * raw_name_)
    {
        String init_name = raw_name_;
        boost::to_lower(init_name);
        size_t underscore_pos = init_name.find('_');
        String init_command = init_name.substr(0, underscore_pos);
        boost::to_upper(init_command);
        bool init_is_check = (std::string_view{init_name}.substr(underscore_pos + 1) == "check");
        return RowPolicyConditionTypeInfo{raw_name_, std::move(init_name), std::move(init_command), init_is_check};
    };

    switch (type_)
    {
        case RowPolicyConditionType::SELECT_FILTER:
        {
            static const auto info = make_info("SELECT_FILTER");
            return info;
        }
#if 0 /// Row-level security for INSERT, UPDATE, DELETE is not implemented yet.
        case RowPolicyConditionType::INSERT_CHECK:
        {
            static const auto info = make_info("INSERT_CHECK");
            return info;
        }
        case RowPolicyConditionType::UPDATE_FILTER:
        {
            static const auto info = make_info("UPDATE_FILTER");
            return info;
        }
        case RowPolicyConditionType::UPDATE_CHECK:
        {
            static const auto info = make_info("UPDATE_CHECK");
            return info;
        }
        case RowPolicyConditionType::DELETE_FILTER:
        {
            static const auto info = make_info("DELETE_FILTER");
            return info;
        }
#endif
        case RowPolicyConditionType::MAX: break;
    }
    throw Exception("Unknown type: " + std::to_string(static_cast<size_t>(type_)), ErrorCodes::LOGICAL_ERROR);
}

inline String toString(RowPolicyConditionType type)
{
    return RowPolicyConditionTypeInfo::get(type).raw_name;
}

}
