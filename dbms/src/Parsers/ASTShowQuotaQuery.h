#pragma once

#include <Parsers/ASTQueryWithOutput.h>


namespace DB
{
/** SHOW QUOTA [[name] [WITH KEY = key] | CURRENT | ALL]
  */
class ASTShowQuotaQuery : public ASTQueryWithOutput
{
public:
    String name;
    std::optional<String> key;
    bool current = false;
    bool all = false;

    /** Get the text that identifies this element. */
    String getID(char) const override { return "SHOW QUOTA query"; }

    ASTPtr clone() const override;

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
