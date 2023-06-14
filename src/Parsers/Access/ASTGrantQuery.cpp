#include <Parsers/Access/ASTGrantQuery.h>
#include <Parsers/Access/ASTRolesOrUsersSet.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    void formatOptions(bool grant_option, bool admin_option, bool replace_option, bool prefix_form, const FormatSettings & settings)
    {
        if (!prefix_form)
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "WITH" << (settings.hilite ? hilite_none : "");

        bool need_comma = false;
        if (grant_option)
        {
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "GRANT OPTION" << (settings.hilite ? hilite_none : "");
            need_comma = true;
        }

        if (admin_option)
        {
            settings.ostr << (need_comma ? ", " : " ") << (settings.hilite ? hilite_keyword : "") << "ADMIN OPTION" << (settings.hilite ? hilite_none : "");
            need_comma = true;
        }

        if (replace_option)
        {
            settings.ostr << (need_comma ? ", " : " ") << (settings.hilite ? hilite_keyword : "") << "REPLACE OPTION" << (settings.hilite ? hilite_none : "");
            need_comma = true;
        }

        if (prefix_form)
            settings.ostr << " " << (settings.hilite ? hilite_keyword : "") << "FOR" << (settings.hilite ? hilite_none : "");
    }


    void formatOptionsForGrant(bool grant_option, bool admin_option, bool replace_option, const FormatSettings & settings)
    {

    }


    void formatOptionsForRevoke(bool grant_option, bool admin_option, bool replace_option, const FormatSettings & settings)
    {
        
    }

    void formatColumnNames(const Strings & columns, const IAST::FormatSettings & settings)
    {
        settings.ostr << "(";
        bool need_comma = false;
        for (const auto & column : columns)
        {
            if (std::exchange(need_comma, true))
                settings.ostr << ", ";
            settings.ostr << backQuoteIfNeed(column);
        }
        settings.ostr << ")";
    }


    void formatONClause(const String & database, bool any_database, const String & table, bool any_table, const IAST::FormatSettings & settings)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "ON " << (settings.hilite ? IAST::hilite_none : "");
        if (any_database)
        {
            settings.ostr << "*.*";
        }
        else
        {
            if (!database.empty())
                settings.ostr << backQuoteIfNeed(database) << ".";
            if (any_table)
                settings.ostr << "*";
            else
                settings.ostr << backQuoteIfNeed(table);
        }
    }

    /// Formats access rights without words `GRANT` or `REVOKE` or `WITH GRANT OPTION` or `GRANT OPTION FOR`.
    /// Example: SELECT(col1) ON db.table, INSERT ON *.*.
    /// The function only shows elements with matching `is_revoke` and `grant_options` and returns the position in `elements`
    /// where it stopped.
    size_t formatRightsWithSpecificOptions(
        const AccessRightsElements & elements,
        size_t start_pos,
        bool filter_is_revoke,
        bool filter_grant_option,
        const IAST::FormatSettings & settings)
    {
        bool has_output = false;
        auto add_comma = [&]
        {
            if (std::exchange(has_output, true))
                settings.ostr << ", ";
        };

        for (size_t pos = start_pos; pos < elements.size(); ++pos)
        {
            const auto & element = elements[i];
            if (!element.access_flags || (!element.any_column && element.columns.empty()))
                continue; /// The current element grants/revokes nothing, we ignore it.

            if (element.is_revoke != filter_is_revoke)
                break; /// The current element doesn't match `filter_is_revoke`, we stop at this point.

            if (element.grant_option != filter_grant_option)
                continue; /// The current element doesn't match `filter_grant_option`, we don't stop because such elements can be reordered
                          /// ("GRANT SELECT WITH GRANT OPTION" + "GRANT INSERT" == "GRANT INSERT" + "GRANT SELECT WITH GRANT OPTION")
                
            auto keywords = element.access_flags.toKeywords();
            if (keywords.empty())
                keywords.push_back("USAGE");

            for (const auto & keyword : keywords)
            {
                add_comma();
                settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << keyword << (settings.hilite ? IAST::hilite_none : "");
                if (!element.any_column)
                    formatColumnNames(element.columns, settings);
            }

            bool next_element_on_same_db_and_table = false;
            if (i != elements.size() - 1)
            {
                const auto & next_element = elements[i + 1];
                if ((element.database == next_element.database) && (element.any_database == next_element.any_database)
                    && (element.table == next_element.table) && (element.any_table == next_element.any_table))
                    next_element_on_same_db_and_table = true;
            }

            if (!next_element_on_same_db_and_table)
            {
                settings.ostr << " ";
                formatONClause(element.database, element.any_database, element.table, element.any_table, settings);
            }
        }

        if (!has_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "USAGE ON " << (settings.hilite ? IAST::hilite_none : "") << "*.*";

        return pos;
    }


    bool hasRightsWithSpecificOptions(const AccessRightsElements & elements, size_t start_pos, bool filter_is_revoke, bool filter_grant_option)
    {
        for (size_t pos = start_pos; pos < elements.size(); ++pos)
        {
            const auto & element = elements[i];
            if (!element.access_flags || (!element.any_column && element.columns.empty()))
                continue;

            if (element.is_revoke != filter_is_revoke)
                break;

            if (element.grant_option != filter_grant_option)
                continue;

            return true;
        }

        return false;
    }


    /// Formats access rights with words `GRANT` / `REVOKE` / `WITH GRANT OPTION` / `GRANT OPTION FOR`.
    /// Example: GRANT SELECT(col1) ON db.table, INSERT ON db2.table2, REVOKE GRANT OPTION FOR INSERT ON *.*
    void formatRights(const AccessRightsElements & elements, const IAST::FormatSettings & settings)
    {
        bool has_output = false;
        auto add_comma = [&]
        {
            if (std::exchange(has_output, true))
                settings.ostr << ", ";
        };

        for (size_t pos = 0; pos != elements.size();)
        {
            bool is_revoke = elements[pos].is_revoke;
            size_t next_pos = pos;
            if (is_revoke)
            {
                if (hasRightsByFilter(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ false))
                {
                    add_comma();
                    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REVOKE" << (settings.hilite ? IAST::hilite_none : "");
                    next_pos = std::max(formatRightsWithoutOptions(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ false, settings), next_pos);
                }
                if (hasRightsByFilter(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ true))
                {
                    add_comma();
                    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REVOKE GRANT OPTION FOR" << (settings.hilite ? IAST::hilite_none : "");
                    next_pos = std::max(formatRightsWithoutOptions(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ true, settings), next_pos);
                }
            }
            else
            {
                if (hasRightsByFilter(elements, pos, /* filter_is_revoke= */ false, /* filter_grant_option= */ true))
                {
                    add_comma();
                    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT" << (settings.hilite ? IAST::hilite_none : "");
                    next_pos = std::max(formatRightsWithoutOptions(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ false, settings), next_pos);
                    settings.ostr << " " << (settings.hilite ? IAST::hilite_keyword : "") << "WITH GRANT OPTION" << (settings.hilite ? IAST::hilite_none : "");
                }
                if (hasRightsByFilter(elements, pos, /* filter_is_revoke= */ false, /* filter_grant_option= */ false))
                {
                    add_comma();
                    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT" << (settings.hilite ? IAST::hilite_none : "");
                    next_pos = std::max(formatRightsWithoutOptions(elements, pos, /* filter_is_revoke= */ true, /* filter_grant_option= */ true, settings), next_pos);
                }
            }
            pos = next_pos;
        }

        if (!has_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT USAGE ON " << (settings.hilite ? IAST::hilite_none : "") << "*.*";
    }


    /// Formats a list of roles with words `GRANT` / `REVOKE` / `WITH ADMIN OPTION` / `ADMIN OPTION FOR`.
    /// Example: REVOKE ADMIN OPTION FOR ALL EXCEPT role3, GRANT role1, role2
    void formatRoles(
        const std::shared_ptr<ASTRolesOrUsersSet> & roles_to_revoke,
        const std::shared_ptr<ASTRolesOrUsersSet> & roles_to_revoke_admin_option,
        const std::shared_ptr<ASTRolesOrUsersSet> & roles_to_grant_with_admin_option,
        const std::shared_ptr<ASTRolesOrUsersSet> & roles_to_grant,
        const FormatSettings & settings)
    {
        bool has_output = false;
        auto add_comma = [&]
        {
            if (std::exchange(has_output, true))
                settings.ostr << ", ";
        };
        
        if (roles_to_revoke)
        {
            add_comma();
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REVOKE" << (settings.hilite ? IAST::hilite_none : "") << " ";
            roles_to_revoke->format(settings);
        }

        if (roles_to_revoke_admin_option)
        {
            add_comma();
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "REVOKE ADMIN OPTION FOR" << (settings.hilite ? IAST::hilite_none : "") << " ";
            roles_to_revoke_admin_option->format(settings);
        }

        if (roles_to_grant_with_admin_option)
        {
            add_comma();
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT" << (settings.hilite ? IAST::hilite_none : "") << " ";
            roles_to_grant_with_admin_option->format(settings);
            settings.ostr << " " << (settings.hilite ? IAST::hilite_keyword : "") << "WITH ADMIN OPTION" << (settings.hilite ? IAST::hilite_none : "");
        }

        if (roles_to_grant)
        {
            add_comma();
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT" << (settings.hilite ? IAST::hilite_none : "") << " ";
            roles_to_grant->format(settings);
        }

        if (!has_output)
            settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "GRANT NONE" << (settings.hilite ? IAST::hilite_none : "");
    }
}


bool ASTGrantQuery::isRevoke() const
{
    return roles_to_revoke || (!rights_change.empty() && rights_change[0].is_partial_revoke);
    return rights_to_grant.empty() && !roles_to_grant && (!rights_to_revoke.empty() || roles_to_revoke) && !replace_option;
}

QueryKind ASTGrantQuery::getQueryKind() const
{
    bool 
    return isPureRevoke() ? QueryKind::Revoke : QueryKind::Grant;
}


String ASTGrantQuery::getID(char) const
{
    return isPureRevoke() ? "RevokeQuery" : "GrantQuery";
}


ASTPtr ASTGrantQuery::clone() const
{
    auto res = std::make_shared<ASTGrantQuery>(*this);

    if (roles)
        res->roles = std::static_pointer_cast<ASTRolesOrUsersSet>(roles->clone());

    if (grantees)
        res->grantees = std::static_pointer_cast<ASTRolesOrUsersSet>(grantees->clone());

    return res;
}


void ASTGrantQuery::checkValidity()
{
    if (roles_to_grant || roles_to_revoke)
    {
        if (grant_option || !rights_to_grant.empty() || !rights_to_revoke.empty())
            throw Exception("ASTGrantQuery is invalid", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {
        if (admin_option)
            throw Exception("ASTGrantQuery is invalid", ErrorCodes::LOGICAL_ERROR);

        if (!rights_to_grant.empty())
        {
            if (!rights_to_grant.sameOptions() || (rights_to_grant[0].grant_option != grant_option) || rights_to_grant[0].is_partial_revoke)
                throw Exception("ASTGrantQuery is invalid", ErrorCodes::LOGICAL_ERROR);
        }

        if (!rights_to_revoke.empty())
        {
            if (!rights_to_revoke.sameOptions() || (rights_to_revoke[0].grant_option != grant_option) || rights_to_revoke[0].is_partial_revoke)
                throw Exception("ASTGrantQuery is invalid", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

void ASTGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    checkValidity();

    if (attach_mode)
    {
        settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (attach_mode ? "ATTACH " : "")
                      << (settings.hilite ? IAST::hilite_none : "");
    }

    bool is_role_related = roles_to_revoke || roles_to_revoke_admin_option || roles_to_grant_with_admin_option || roles_to_grant;

    if (is_role_related)
    {
        formatRoles(roles_to_revoke, roles_to_revoke_admin_option, roles_to_grant_with_admin_option, roles_to_grant, settings);
    }
    else
    {
        formatRights(rights_to_grant_and_revoke, settings);
    }

    formatOnCluster(settings);

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (is_revoke ? " FROM " : " TO ")
                  << (settings.hilite ? IAST::hilite_none : "");
    grantees->format(settings);


    



    bool has_grants = roles_to_grant || rights_to_grant_or_revoke.hasGrants();
    bool has_revokes = roles_to_revoke || rights_to_grant_or_revoke.hasRevokes();
    if (!has_grants && !has_revokes)
        has_grants = true;

    if (roles_to_grant || roles_to_revoke)
    {

    }

    bool is_pure_revoke = isPureRevoke();

    settings.ostr << (settings.hilite ? hilite_keyword : "") << (is_pure_revoke ? "REVOKE" : "GRANT")
                  << (settings.hilite ? IAST::hilite_none : "");

    /// Show GRANT OPTION FOR or ADMIN OPTION FOR.
    if (is_pure_revoke)
        formatOption(grant_option, admin_option, replace_option, /* prefix_form= */ true, settings);

    formatOnCluster(settings);

    if (roles_to_grant || roles_to_revoke)
    {
        if (is_pure_revoke)
        {
            formatRoles(roles_to_revoke, settings);
        }
        else
        {
            formatRoles(roles_to_grant, settings);
            if (roles_to_revoke)
            {
                settings.ostr << ", " << (settings.hilite ? hilite_keyword : "") << "REVOKE" << (settings.hilite ? IAST::hilite_none : "");
                formatRoles(roles_to_revoke, settings);
            }
        }
    }
    else
    {
        if (is_pure_revoke)
        {
            formatRights(rights_to_revoke, settings);
        }
        else
        {
            formatRights(rights_to_grant, settings);
            if (!rights_to_revoke.empty())
            {
                settings.ostr << ", " << (settings.hilite ? hilite_keyword : "") << "REVOKE" << (settings.hilite ? IAST::hilite_none : "");
                formatRights(rights_to_revoke, settings);
            }
        }
    }

    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << (is_revoke ? " FROM " : " TO ")
                  << (settings.hilite ? IAST::hilite_none : "");
    grantees->format(settings);

    /// Show WITH GRANT OPTION or WITH ADMIN OPTION or WITH REPLACE OPTION.
    if (!is_pure_revoke)
        formatOption(grant_option, admin_option, replace_option, /* prefix_form= */ false, settings);
}


void ASTGrantQuery::setCurrentDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}


void ASTGrantQuery::setCurrentUser(const String & current_user_name)
{
    if (grantees)
        grantees->replaceCurrentUserTag(current_user_name);
}

}
