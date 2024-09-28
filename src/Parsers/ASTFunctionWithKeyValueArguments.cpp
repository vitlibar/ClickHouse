#include <Parsers/ASTFunctionWithKeyValueArguments.h>

#include <Poco/String.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{

String ASTPair::getID(char) const
{
    return "pair";
}


ASTPtr ASTPair::clone() const
{
    auto res = std::make_shared<ASTPair>(*this);
    res->children.clear();
    res->set(res->second, second->clone());
    return res;
}


void ASTPair::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword(Poco::toUpper(first));
    out.ostr << " ";

    if (second_with_brackets)
        out.writeKeyword("(");

    if (!out.shouldShowSecrets() && (first == "password"))
    {
        /// Hide password in the definition of a dictionary:
        /// SOURCE(CLICKHOUSE(host 'example01-01-1' port 9000 user 'default' password '[HIDDEN]' db 'default' table 'ids'))
        out.writeSecret();
    }
    else
    {
        second->formatImpl(out);
    }

    if (second_with_brackets)
        out.writeKeyword(")");
}


bool ASTPair::hasSecretParts() const
{
    return first == "password";
}


void ASTPair::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(first.size());
    hash_state.update(first);
    hash_state.update(second_with_brackets);
    IAST::updateTreeHashImpl(hash_state);
}


String ASTFunctionWithKeyValueArguments::getID(char delim) const
{
    return "FunctionWithKeyValueArguments " + (delim + name);
}


ASTPtr ASTFunctionWithKeyValueArguments::clone() const
{
    auto res = std::make_shared<ASTFunctionWithKeyValueArguments>(*this);
    res->children.clear();

    if (elements)
    {
        res->elements = elements->clone();
        res->children.push_back(res->elements);
    }

    return res;
}


void ASTFunctionWithKeyValueArguments::formatImpl(FormattingBuffer out) const
{
    out.writeKeyword(Poco::toUpper(name));
    out.ostr << (has_brackets ? "(" : "");
    elements->formatImpl(out);
    out.ostr << (has_brackets ? ")" : "");
}


void ASTFunctionWithKeyValueArguments::updateTreeHashImpl(SipHash & hash_state) const
{
    hash_state.update(name.size());
    hash_state.update(name);
    hash_state.update(has_brackets);
    IAST::updateTreeHashImpl(hash_state);
}

}
