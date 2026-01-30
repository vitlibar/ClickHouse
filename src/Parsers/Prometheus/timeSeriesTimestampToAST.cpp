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
    boost::intrusive_ptr<ASTFunction> addTimeZone(boost::intrusive_ptr<ASTFunction> && ast, String timezone)
    {
        if (!timezone.empty())
            ast->arguments->children.push_back(make_intrusive<ASTLiteral>(std::move(timezone)));
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
            makeASTFunction("toDateTime64", make_intrusive<ASTLiteral>(str), make_intrusive<ASTLiteral>(scale)),
            getDateTimeTimezone(*timestamp_data_type));
    }
    else if (isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(static_cast<Decimal64>(timestamp), scale);
        return makeASTFunction("toDecimal64", make_intrusive<ASTLiteral>(str), make_intrusive<ASTLiteral>(scale));
    }
    else if (isDateTime(timestamp_data_type))
    {
        return addTimeZone(
            makeASTFunction("toDateTime", make_intrusive<ASTLiteral>(static_cast<Decimal64>(timestamp))),
            getDateTimeTimezone(*timestamp_data_type));
    }
    else
    {
        return make_intrusive<ASTLiteral>(timestamp.value);
    }
}


ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type)
{
    if (isDateTime64(timestamp_data_type) || isDecimal(timestamp_data_type))
    {
        auto scale = getDecimalScale(*timestamp_data_type);
        String str = toString(duration, scale);
        return makeASTFunction("toDecimal64", make_intrusive<ASTLiteral>(str), make_intrusive<ASTLiteral>(scale));
    }
    else
    {
        return make_intrusive<ASTLiteral>(duration.value);
    }
}

}
