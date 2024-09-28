#pragma once

#include <Parsers/IAST_fwd.h>
#include <Parsers/ASTQueryWithOutput.h>

namespace DB
{

/// Query SHOW COLUMNS
class ASTShowColumnsQuery : public ASTQueryWithOutput
{
public:
    bool extended = false;
    bool full = false;
    bool not_like = false;
    bool case_insensitive_like = false;

    ASTPtr where_expression;
    ASTPtr limit_length;

    String from_database;
    String from_table;

    String like;

    String getID(char) const override { return "ShowColumns"; }
    ASTPtr clone() const override;
    QueryKind getQueryKind() const override { return QueryKind::Show; }

protected:
    void formatQueryImpl(FormattingBuffer out) const override;
};

}
