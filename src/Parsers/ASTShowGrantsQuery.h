#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
class ASTRolesOrUsersSet;
class ASTSettingsProfileElements;

/** SHOW GRANTS
  * SHOW GRANTS FOR role_name [,...]
  * SHOW GRANTS FOR user_name [,...]
  */
class ASTShowGrantsQuery : public ASTQueryWithOutput
{
public:
    std::shared_ptr<ASTRolesOrUsersSet> for_whom;

    String getID(char) const override;
    ASTPtr clone() const override;
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
