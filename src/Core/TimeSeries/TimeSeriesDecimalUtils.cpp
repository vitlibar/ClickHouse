#include <Core/TimeSeries/TimeSeriesDecimalUtils.h>

#include <Common/quoteString.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/PrometheusQueryParsingUtil.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


namespace
{
    template <typename DecimalType>
    DecimalField<DecimalType> extractFromField(const Field & field, const DataTypePtr & type, UInt32 default_scale)
    {
        constexpr bool result_is_timestamp = std::is_same_v<DecimalType, DateTime64>;
        constexpr std::string_view what = result_is_timestamp ? "timestamp" : "time interval";

        switch (field.getType())
        {
            case Field::Types::Int64:
            {
                auto value = field.safeGet<Int64>();
                if (const auto * interval_type = checkAndGetDataType<DataTypeInterval>(type.get()))
                {
                    switch(interval_type->getKind())
                    {
                        case IntervalKind::Kind::Nanosecond: return DecimalField<DecimalType>{value, 9};
                        case IntervalKind::Kind::Microsecond: return DecimalField<DecimalType>{value, 6};
                        case IntervalKind::Kind::Millisecond: return DecimalField<DecimalType>{value, 3};
                        case IntervalKind::Kind::Second: return DecimalField<DecimalType>{value, 0};
                        case IntervalKind::Kind::Minute: return DecimalField<DecimalType>{value * 60, 0};
                        case IntervalKind::Kind::Hour: return DecimalField<DecimalType>{value * (60 * 60), 0};
                        case IntervalKind::Kind::Day: return DecimalField<DecimalType>{value * (24 * 60 * 60), 0};
                        case IntervalKind::Kind::Week: return DecimalField<DecimalType>{value * (7 * 24 * 60 * 60), 0};
                        case IntervalKind::Kind::Year: return DecimalField<DecimalType>{value * (365 * 24 * 60 * 60), 0};
                        default:
                        {
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot extract a {} from an interval of type {}",
                                            what, IntervalKind{interval_type->getKind()}.toString());
                        }
                    }
                }
                else
                {
                    return DecimalField<DecimalType>{value, 0};
                }
            }
            case Field::Types::UInt64:
            {
                return DecimalField<DecimalType>{field.safeGet<UInt64>(), 0};
            }
            case Field::Types::Float64:
            {
                auto float_value = field.safeGet<Float64>();
                auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                return DecimalField<DecimalType>{static_cast<Int64>(float_value * scale_multiplier + 0.5), default_scale};
            }
            case Field::Types::Decimal32:
            {
                auto decimal32 = field.safeGet<Decimal32>();
                return DecimalField<DecimalType>{decimal32.getValue(), decimal32.getScale()};
            }
            case Field::Types::Decimal64:
            {
                return field.safeGet<DecimalType>();
            }
            case Field::Types::String:
            {
                const auto & str = field.safeGet<String>();
                PrometheusQueryParsingUtil::ScalarOrInterval scalar_or_interval;
                String error_message;
                size_t error_pos;
                if (PrometheusQueryParsingUtil::parseScalarOrInterval(str, scalar_or_interval, error_message, error_pos))
                {
                    if (scalar_or_interval.interval)
                    {
                        const auto & decimal64 = *scalar_or_interval.interval;
                        return DecimalField<DecimalType>{decimal64.getValue(), decimal64.getScale()};
                    }
                    else
                    {
                        auto scale_multiplier = DecimalUtils::scaleMultiplier<Int64>(default_scale);
                        return DecimalField<DecimalType>{static_cast<Int64>(*scalar_or_interval.scalar * scale_multiplier + 0.5), default_scale};
                    }
                }
                if constexpr (result_is_timestamp)
                {
                    DateTime64 datetime;
                    ReadBufferFromString buf{str};
                    if (tryReadDateTime64Text(datetime, default_scale, buf))
                    {
                        return DecimalField<DateTime64>{datetime, default_scale};
                    }
                }
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse a {} from string {}: {}",
                                what, quoteString(str), error_message);
            }
            default:
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Cannot extract a {} from a field of type {}",
                                what, field.getType());
            }
        }
    }
}


UInt32 getTimeSeriesTimestampScale(const DataTypePtr & timestamp_type)
{
    if (isDecimal(timestamp_type) || isDateTime64(timestamp_type))
        return getDecimalScale(*timestamp_type);
    else
        return 0;
}


DecimalField<DateTime64> getTimeSeriesTimestamp(const Field & field, UInt32 default_scale)
{
    return getTimeSeriesTimestamp(field, nullptr, default_scale);
}

DecimalField<DateTime64> getTimeSeriesTimestamp(const Field & field, const DataTypePtr & type, UInt32 default_scale)
{
    return extractFromField<DateTime64>(field, type, default_scale);
}

DecimalField<Decimal64> getTimeSeriesInterval(const Field & field, UInt32 default_scale)
{
    return getTimeSeriesInterval(field, nullptr, default_scale);
}

DecimalField<Decimal64> getTimeSeriesInterval(const Field & field, const DataTypePtr & type, UInt32 default_scale)
{
    return extractFromField<Decimal64>(field, type, default_scale);
}


ASTPtr timeSeriesTimestampToAST(const DecimalField<DateTime64> & timestamp, const DataTypePtr & timestamp_type)
{
    switch (WhichDataType{timestamp_type}.idx)
    {
        case TypeIndex::DateTime64:
        {
            UInt32 scale = getDecimalScale(*timestamp_type);
            Decimal64 value = DecimalUtils::convertTo<Decimal64>(scale, timestamp.getValue(), timestamp.getScale());
            String str = toString(value, scale);
            /// toDateTime64() doesn't accept an integer as its first argument, so we convert it to a floating-point.
            if (str.find_first_of(".eE") == String::npos)
                str += ".";
            auto function = makeASTFunction("toDateTime64", std::make_shared<ASTLiteral>(str), std::make_shared<ASTLiteral>(scale));
            auto timezone = getDateTimeTimezone(*timestamp_type);
            if (!timezone.empty())
                function->arguments->children.push_back(std::make_shared<ASTLiteral>(timezone));
            return function;
        }

        case TypeIndex::DateTime:
        {
            UInt32 value = timestamp.getValue() / DecimalUtils::scaleMultiplier<Decimal64>(timestamp.getScale());
            auto function = makeASTFunction("toDateTime", std::make_shared<ASTLiteral>(value));
            auto timezone = getDateTimeTimezone(*timestamp_type);
            if (!timezone.empty())
                function->arguments->children.push_back(std::make_shared<ASTLiteral>(timezone));
            return function;
        }

        case TypeIndex::UInt32:
        {
            UInt32 value = timestamp.getValue() / DecimalUtils::scaleMultiplier<Decimal64>(timestamp.getScale());
            return makeASTFunction("toUInt32", std::make_shared<ASTLiteral>(value));
        }

        default:
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unexpected type of the `timestamp` column: {}", timestamp_type->getName());
        }
    }
}

ASTPtr timeSeriesIntervalToAST(const DecimalField<Decimal64> & interval)
{
    return std::make_shared<ASTLiteral>(interval);
}


DecimalField<DateTime64> addTimeSeriesInterval(const DecimalField<DateTime64> & left, const DecimalField<Decimal64> & right)
{
    auto max_scale = std::max(left.getScale(), right.getScale());
    auto scaled_left = left.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - left.getScale());
    auto scaled_right = right.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - right.getScale());
    return DecimalField<DateTime64>{scaled_left + scaled_right, max_scale};
}

DecimalField<DateTime64> subtractTimeSeriesInterval(const DecimalField<DateTime64> & left, const DecimalField<Decimal64> & right)
{
    auto max_scale = std::max(left.getScale(), right.getScale());
    auto scaled_left = left.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - left.getScale());
    auto scaled_right = right.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - right.getScale());
    return DecimalField<DateTime64>{scaled_left - scaled_right, max_scale};
}

DecimalField<Decimal64> getTimeSeriesInterval(const DecimalField<DateTime64> & min_time, const DecimalField<DateTime64> & max_time)
{
    auto max_scale = std::max(min_time.getScale(), max_time.getScale());
    auto scaled_min_time = min_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - min_time.getScale());
    auto scaled_max_time = max_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - max_time.getScale());
    return DecimalField<Decimal64>{scaled_max_time - scaled_min_time, max_scale};
}


size_t getNumberOfTimeSeriesSteps(
    const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time, const DecimalField<Decimal64> & step)
{
    UInt32 max_scale = std::max({start_time.getScale(), end_time.getScale(), step.getScale()});

    Int64 scaled_start_time = start_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - start_time.getScale());
    Int64 scaled_end_time = end_time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - end_time.getScale());
    Int64 scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());

    if (scaled_start_time == scaled_end_time)
        return 1;

    if (scaled_end_time < scaled_start_time)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "End timestamp is less than start timestamp");
    
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");

    return (scaled_end_time - scaled_start_time) / scaled_step + 1;
}


DecimalField<DateTime64> roundUpTimeSeriesTimestamp(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
{
    auto max_scale = std::max(time.getScale(), step.getScale());
    auto scaled_time = time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - time.getScale());
    auto scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = scaled_time % scaled_step;
    if (!x)
        return time;
    return DecimalField<DateTime64>{scaled_time + scaled_step - x, max_scale};
}


DecimalField<DateTime64> roundDownTimeSeriesTimestamp(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step)
{
    auto max_scale = std::max(time.getScale(), step.getScale());
    auto scaled_time = time.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - time.getScale());
    auto scaled_step = step.getValue() * DecimalUtils::scaleMultiplier<Int64>(max_scale - step.getScale());
    if (scaled_step <= 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Step should be greater than zero");
    auto x = scaled_time % scaled_step;
    if (!x)
        return time;
    return DecimalField<DateTime64>{scaled_time - x, max_scale};
}

}
