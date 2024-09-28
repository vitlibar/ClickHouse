#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>


namespace DB
{

ASTPtr ASTSelectIntersectExceptQuery::clone() const
{
    auto res = std::make_shared<ASTSelectIntersectExceptQuery>(*this);

    res->children.clear();
    for (const auto & child : children)
        res->children.push_back(child->clone());

    res->final_operator = final_operator;
    return res;
}

void ASTSelectIntersectExceptQuery::formatImpl(FormattingBuffer out) const
{
    for (ASTs::const_iterator it = children.begin(); it != children.end(); ++it)
    {
        if (it != children.begin())
        {
            out.nlOrWs();
            out.writeIndent();
            out.writeKeyword(fromOperator(final_operator));
            out.nlOrWs();
        }

        (*it)->formatImpl(out);
    }
}

ASTs ASTSelectIntersectExceptQuery::getListOfSelects() const
{
    /**
     * Because of normalization actual number of selects is 2.
     * But this is checked in InterpreterSelectIntersectExceptQuery.
     */
    ASTs selects;
    for (const auto & child : children)
    {
        if (typeid_cast<ASTSelectQuery *>(child.get())
            || typeid_cast<ASTSelectWithUnionQuery *>(child.get())
            || typeid_cast<ASTSelectIntersectExceptQuery *>(child.get()))
            selects.push_back(child);
    }
    return selects;
}

const char * ASTSelectIntersectExceptQuery::fromOperator(Operator op)
{
    switch (op)
    {
        case Operator::EXCEPT_ALL:
            return "EXCEPT ALL";
        case Operator::EXCEPT_DISTINCT:
            return "EXCEPT DISTINCT";
        case Operator::INTERSECT_ALL:
            return "INTERSECT ALL";
        case Operator::INTERSECT_DISTINCT:
            return "INTERSECT DISTINCT";
        default:
            return "";
    }
}
}
