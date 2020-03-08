#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
class ParserAccessEntityReference : public IParserBase
{
public:
    ParserAccessEntityReference & enableIDMode(bool enable_) { id_mode = enable_; return *this; }

protected:
    const char * getName() const override { return "AccessEntityReference"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

private:
    bool id_mode = false;
};

}
