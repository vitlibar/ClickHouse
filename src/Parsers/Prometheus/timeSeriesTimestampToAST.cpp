#include <Parsers/Prometheus/timeSeriesTimestampToAST.h>

#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesDecimal.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace
{
    /// Adds a timezone to the list of argument of the function `toDateTime64()` or `toDateTime()`.
    std::shared_ptr<ASTFunction> addTimeZone(std::shared_ptr<ASTFunction> && ast, String timezone)
    {
        if (!timezone.empty())
            ast->arguments->children.push_back(std::make_shared<ASTLiteral>(std::move(timezone)));
        return ast;
    }
}

ASTPtr timeSeriesTimestampToAST(DateTime64 timestamp, const DataTypePtr & timestamp_data_type)
{
    if (isDateTime64(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(static_cast<Decimal64>(timestamp), scale);
        /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point number.
        if (str.find_first_of(".eE") == String::npos)
            str += ".";
        return addTimeZone(
            makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale)),
            getDateTimeTimezone(*timestamp_data_type));
    }
    else if (isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(static_cast<Decimal64>(timestamp), scale);
        return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
    }
    else if (isDateTime(timestamp_data_type))
    {
        return addTimeZone(
            makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(static_cast<Decimal64>(timestamp))),
            getDateTimeTimezone(*timestamp_data_type));
    }
    else
    {
        return std::make_shared<ASTLiteral>(timestamp.value);
    }
}


ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type)
{
    if (isDateTime64(timestamp_data_type) || isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(duration, scale);
        return makeASTFunction("toDecimal64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
    }
    else
    {
        return std::make_shared<ASTLiteral>(duration.value);
    }
}

}
