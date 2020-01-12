#include <Access/AccessRightsElement.h>
#include <Common/quoteString.h>


namespace DB
{
AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_)
    : access_flags(access_flags_), database(database_), any_database(false)
{
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_)
    : access_flags(access_flags_), database(database_), table(table_), any_database(false), any_table(false)
{
}


AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const std::string_view & column_)
    : access_flags(access_flags_)
    , database(database_)
    , table(table_)
    , columns({String{column_}})
    , any_database(false)
    , any_table(false)
    , any_column(false)
{
}


AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const std::vector<std::string_view> & columns_)
    : access_flags(access_flags_)
    , database(database_)
    , table(table_)
    , any_database(false)
    , any_table(false)
    , any_column(false)
{
    columns.resize(columns_.size());
    for (size_t i = 0; i != columns_.size(); ++i)
        columns[i] = String{columns_[i]};
}


AccessRightsElement::AccessRightsElement(
    AccessFlags access_flags_, const std::string_view & database_, const std::string_view & table_, const Strings & columns_)
    : access_flags(access_flags_)
    , database(database_)
    , table(table_)
    , columns(columns_)
    , any_database(false)
    , any_table(false)
    , any_column(false)
{
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_)
    : access_flags(access_flags_), table(table_), any_database(false), current_database(true), any_table(false)
{
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const std::string_view & column_)
    : access_flags(access_flags_)
    , table(table_)
    , columns({String{column_}})
    , any_database(false)
    , current_database(true)
    , any_table(false)
    , any_column(false)
{
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const std::vector<std::string_view> & columns_)
    : access_flags(access_flags_)
    , table(table_)
    , any_database(false)
    , current_database(true)
    , any_table(false)
    , any_column(false)
{
    columns.resize(columns_.size());
    for (size_t i = 0; i != columns_.size(); ++i)
        columns[i] = String{columns_[i]};
}


AccessRightsElement::AccessRightsElement(AccessFlags access_flags_, CurrentDatabaseTag, const std::string_view & table_, const Strings & columns_)
    : access_flags(access_flags_)
    , table(table_)
    , columns(columns_)
    , any_database(false)
    , current_database(true)
    , any_table(false)
    , any_column(false)
{
}


void AccessRightsElement::setDatabase(const String & database_)
{
    database = database_;
    current_database = false;
    any_database = false;
}


void AccessRightsElement::setDatabase(CurrentDatabaseTag)
{
    database.clear();
    current_database = true;
    any_database = false;
}


void AccessRightsElement::replaceDatabase(const String & old_database_, const String & new_database_)
{
    if (!current_database && !any_database && (database == old_database_))
        setDatabase(new_database_);
}

void AccessRightsElement::replaceDatabase(CurrentDatabaseTag, const String & new_database_)
{
    if (current_database)
        setDatabase(new_database_);
}

void AccessRightsElement::replaceDatabase(const String & old_database_, CurrentDatabaseTag)
{
    if (!current_database && !any_database && (database == old_database_))
        setDatabase(CurrentDatabaseTag{});
}


String AccessRightsElement::toString() const
{
    String columns_in_parentheses;
    if (!any_column)
    {
        for (const auto & column : columns)
        {
            columns_in_parentheses += columns_in_parentheses.empty() ? "(" : ", ";
            columns_in_parentheses += backQuoteIfNeed(column);
        }
        columns_in_parentheses += ")";
    }

    String msg;
    for (const std::string_view & keyword : access_flags.toKeywords())
    {
        if (!msg.empty())
            msg += ", ";
        msg += String{keyword} + columns_in_parentheses;
    }

    msg += " ON ";

    if (any_database)
        msg += "*.";
    else if (!current_database && !database.empty())
        msg += backQuoteIfNeed(database) + ".";

    if (any_table)
        msg += "*";
    else
        msg += backQuoteIfNeed(table);
    return msg;
}


void AccessRightsElements::replaceDatabase(const String & old_database_, const String & new_database_)
{
    for (auto & element : *this)
        element.replaceDatabase(old_database_, new_database_);
}


void AccessRightsElements::replaceDatabase(CurrentDatabaseTag, const String & new_database_)
{
    for (auto & element : *this)
        element.replaceDatabase(CurrentDatabaseTag{}, new_database_);
}


void AccessRightsElements::replaceDatabase(const String & old_database_, CurrentDatabaseTag)
{
    for (auto & element : *this)
        element.replaceDatabase(old_database_, CurrentDatabaseTag{});
}


String AccessRightsElements::toString() const
{
    String res;
    bool need_comma = false;
    for (auto & element : *this)
    {
        if (std::exchange(need_comma, true))
            res += ", ";
        res += element.toString();
    }

    if (res.empty())
        res = "USAGE ON *.*";
    return res;
}

}
