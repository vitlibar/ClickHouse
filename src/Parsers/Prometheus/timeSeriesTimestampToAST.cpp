#include <Parsers/Prometheus/timeSeriesTimestampToAST.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

ASTPtr timeSeriesTimestampToAST(DateTime64 timestamp, const DataTypePtr & timestamp_data_type)
{
    UInt32 scale = 0;
    if (isDateTime64(timestamp_data_type))
        scale = getDecimalScale(*timestamp_data_type);

    std::shared_ptr<ASTFunction> ast;
    if (scale == 0)
    {
        ast = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(static_cast<Decimal64>(timestamp)));
    }
    else
    {
        String str = toString(static_cast<Decimal64>(timestamp), scale);
        /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point.
        if (str.find_first_of(".eE") == String::npos)
            str += ".";
        ast = makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
    }

    if (isDateTimeOrDateTime64(timestamp_data_type))
    {
        auto timezone = getDateTimeTimezone(*timestamp_data_type);
        if (!timezone.empty())
            ast->arguments->children.push_back(std::make_shared<ASTLiteral>(timezone));
    }

    return ast;
}


ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type)
{
    UInt32 scale = 0;
    if (isDateTime64(timestamp_data_type))
        scale = getDecimalScale(*timestamp_data_type);

    if (scale == 0)
        return std::make_shared<ASTLiteral>(static_cast<Int64>(duration));
    else
        return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(toString(duration, scale)), std::make_shared<ASTLiteral>(scale));
}

}
