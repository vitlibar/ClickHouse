#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
enum class TargetTableKind;

/// Parses target tables for a table - for example the destination table for a materialized view.
class ParserTargetTables : public IParserBase
{
public:
    explicit ParserTargetTables(std::optional<TargetTableKind> accept_kinds_min_ = {}, std::optional<TargetTableKind> accept_kinds_max_ = {})
        : accept_kinds_min(accept_kinds_min_), accept_kinds_max(accept_kinds_max_) {}

protected:
    const char * getName() const override { return "target tables"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    std::optional<TargetTableKind> accept_kinds_min;
    std::optional<TargetTableKind> accept_kinds_max;
};

}
