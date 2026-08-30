#pragma once

#include <Parsers/ASTExpressionList.h>

namespace Poco::JSON { class Object; }

namespace DB
{

/// AST for data types, e.g. UInt8 or Tuple(x UInt8, y Enum(a = 1))
class ASTDataType : public IAST
{
public:
    String name;

    /// Optional codecs of type arguments: Map(String CODEC(ZSTD(1)), Float64 CODEC(Gorilla, ZSTD(1))).
    /// If non-empty, has the same size as the argument list; an entry is nullptr for an argument
    /// without a codec. The parser fills it only for the Map type (for its key and value).
    /// Like ASTTupleDataType::element_codecs, such codecs are not part of the data type: they are
    /// allowed only for columns of CREATE TABLE and ALTER TABLE queries, where they are extracted
    /// from the type AST before the type is created (see extractSubcolumnCodecsFromTypeAST()).
    ASTs argument_codecs;

    String getID(char delim) const override;
    ASTPtr clone() const override;
    void writeJSON(WriteBuffer & out) const override;
    void readJSON(const Poco::JSON::Object & json) override;
    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

    ASTPtr getArguments() const;
    void resetArguments();

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

template <typename... Args>
boost::intrusive_ptr<ASTDataType> makeASTDataType(const String & name, Args &&... args)
{
    auto data_type = make_intrusive<ASTDataType>();
    data_type->name = name;

    if constexpr (sizeof...(args))
    {
        auto arguments = make_intrusive<ASTExpressionList>();
        data_type->children.push_back(arguments);
        arguments->children = {std::forward<Args>(args)...};
    }

    return data_type;
}

}
