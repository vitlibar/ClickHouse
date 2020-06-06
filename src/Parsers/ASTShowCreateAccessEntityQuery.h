#pragma once

#include <Parsers/ASTQueryWithOutput.h>
#include <Access/IAccessEntity.h>


namespace DB
{
class ASTRowPolicyNames;

/** SHOW CREATE QUOTA [name [, name2 ...] | ALL]
  * SHOW CREATE [ROW] POLICY {name ON [database.]table [, name2 ON database2.table2 ...] | ALL}
  * SHOW CREATE USER [name [, name2 ...] | CURRENT_USER | ALL]
  * SHOW CREATE ROLE {name [, name2 ...] | ALL}
  * SHOW CREATE [SETTINGS] PROFILE {name [, name2 ...] | ALL}
  */
class ASTShowCreateAccessEntityQuery : public ASTQueryWithOutput
{
public:
    using EntityType = IAccessEntity::Type;

    EntityType type;
    Strings names;
    bool current_quota = false;
    bool current_user = false;
    std::shared_ptr<ASTRowPolicyNames> row_policy_names;

    bool all = false;
    String all_on_database;
    String all_on_table_name;

    String getID(char) const override;
    ASTPtr clone() const override;

    void replaceEmptyDatabaseWithCurrent(const String & current_database);

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
