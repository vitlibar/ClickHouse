#pragma once

#include <Parsers/IAST.h>
#include <Access/Quota.h>


namespace DB
{
/// Represents a name of a profile/quota.
class ASTAccessEntityReference : public IAST
{
public:
    String name;
    bool id_mode = false;  /// If true then `name` keeps UUID, not a name.

    String getID(char) const override { return "AccessEntityReference"; }
    ASTPtr clone() const override { return std::make_shared<ASTAccessEntityReference>(*this); }
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};
}
