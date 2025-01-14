#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{


class ParserExplainQuery : public IParserBase
{
protected:
    bool allow_settings_after_format_in_insert;
    bool select_only;

    const char * getName() const override { return "EXPLAIN"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
public:
    explicit ParserExplainQuery(bool allow_settings_after_format_in_insert_)
        : allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
        , select_only(false)
    {}

    explicit ParserExplainQuery()
        : allow_settings_after_format_in_insert(false) , select_only(true)
    {}

};

}
