#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserKQLStatement : public IParserBase
{
private:
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL Statement"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserKQLStatement(bool allow_settings_after_format_in_insert_ = false)
        : allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {
    }
};

class ParserKQLWithOutput : public IParserBase
{
protected:
    bool allow_settings_after_format_in_insert;
    const char * getName() const override { return "KQL with output"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    explicit ParserKQLWithOutput(bool allow_settings_after_format_in_insert_ = false)
        : allow_settings_after_format_in_insert(allow_settings_after_format_in_insert_)
    {
    }
};

class ParserKQLWithUnionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "KQL query, possibly with UNION"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

class ParserKQLTableFunction : public IParserBase
{
protected:
    const char * getName() const override { return "KQL function"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
