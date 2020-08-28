#include <Functions/FunctionsJSON.h>
#include <Functions/FunctionFactory.h>
#include <cctype>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


std::vector<FunctionJSONHelpers::Move> FunctionJSONHelpers::prepareMoves(const char * function_name, Block & block, const ColumnNumbers & arguments, size_t first_index_argument, size_t num_index_arguments)
{
    std::vector<Move> moves;
    moves.reserve(num_index_arguments);
    for (const auto i : ext::range(first_index_argument, first_index_argument + num_index_arguments))
    {
        const auto & column = block.getByPosition(arguments[i]);
        if (!isString(column.type) && !isInteger(column.type))
            throw Exception{"The argument " + std::to_string(i + 1) + " of function " + String(function_name)
                                + " should be a string specifying key or an integer specifying index, illegal type: " + column.type->getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (column.column && isColumnConst(*column.column))
        {
            const auto & column_const = assert_cast<const ColumnConst &>(*column.column);
            if (isString(column.type))
                moves.emplace_back(MoveType::ConstKey, column_const.getValue<String>());
            else
                moves.emplace_back(MoveType::ConstIndex, column_const.getInt(0));
        }
        else
        {
            if (isString(column.type))
                moves.emplace_back(MoveType::Key, "");
            else
                moves.emplace_back(MoveType::Index, 0);
        }
    }
    return moves;
}

std::vector<FunctionJSONHelpers::Move> FunctionJSONHelpers::prepareMovesByPtr(const char * function_name, Block & block, const ColumnNumbers & arguments, size_t first_index_argument, size_t num_index_arguments)
{
    if (num_index_arguments != 1)
    {
        throw Exception{"The function " + String(function_name) + " expects "
                            + std::to_string(arguments.size() - num_index_arguments + 1) + " arguments, passed "
                            + std::to_string(arguments.size()) + " arguments",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};
    }

    const auto & column = block.getByPosition(arguments[first_index_argument]);
    if (!isString(column.type) || !column.column || !isColumnConst(*column.column))
        throw Exception{"The argument " + std::to_string(first_index_argument) + " of function " + String(function_name)
                            + " should be a constant string specifying a JSON Pointer, illegal type: " + column.type->getName(),
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    const auto & json_pointer = assert_cast<const ColumnConst &>(*column.column).getValue<const String &>();
    std::string_view unparsed_json_pointer = json_pointer;
    std::vector<Move> moves;

    while (!unparsed_json_pointer.empty())
    {
        if (!unparsed_json_pointer.starts_with('/'))
            throw Exception("JSON pointer should starts with '/'", ErrorCodes::BAD_ARGUMENTS);
        unparsed_json_pointer.remove_prefix(1);
        size_t next_slash = unparsed_json_pointer.find('/');
        std::string_view component = unparsed_json_pointer.substr(0, next_slash);
        if (next_slash == std::string::npos)
            unparsed_json_pointer = {};
        else
            unparsed_json_pointer.remove_prefix(next_slash);

        bool digits_only = !component.empty() && std::all_of(component.begin(), component.end(), [](char c) { return std::isdigit(c); })
            && (component.size() <= 1 || component.front() != '0');

        String unescaped_key;
        if (digits_only)
        {
            unescaped_key = component;
        }
        else
        {
            unescaped_key.reserve(component.size());
            while (!component.empty())
            {
                char c = component.front();
                component.remove_prefix(1);
                if (c == '~')
                {
                    if (component.starts_with('0'))
                    {
                        /// '~' is encoded as "~0"
                        component.remove_prefix(1);
                        unescaped_key.push_back('~');
                        continue;
                    }
                    if (component.starts_with('1'))
                    {
                        /// '/' is encoded as "~1"
                        component.remove_prefix(1);
                        unescaped_key.push_back('/');
                        continue;
                    }
                }
                unescaped_key.push_back(c);
            }
        }

        if (digits_only)
        {
            size_t index = parseFromString<size_t>(unescaped_key) + 1;
            moves.emplace_back(MoveType::ConstKeyOrIndex, std::move(unescaped_key), index);
        }
        else
            moves.emplace_back(MoveType::ConstKey, std::move(unescaped_key));
    }
    return moves;
}

size_t FunctionJSONHelpers::calculateMaxSize(const ColumnString::Offsets & offsets)
{
    size_t max_size = 0;
    for (const auto i : ext::range(0, offsets.size()))
    {
        size_t size = offsets[i] - offsets[i - 1];
        if (max_size < size)
            max_size = size;
    }
    if (max_size)
        --max_size;
    return max_size;
}


void registerFunctionsJSON(FunctionFactory & factory)
{
    factory.registerFunction<FunctionJSON<NameIsValidJSON, IsValidJSONImpl>>();

    factory.registerFunction<FunctionJSON<NameJSONHas, JSONHasImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONLength, JSONLengthImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONKey, JSONKeyImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONType, JSONTypeImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractInt, JSONExtractInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractUInt, JSONExtractUInt64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractFloat, JSONExtractFloat64Impl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractBool, JSONExtractBoolImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractString, JSONExtractStringImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtract, JSONExtractImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValues, JSONExtractKeysAndValuesImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractRaw, JSONExtractRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractArrayRaw, JSONExtractArrayRawImpl>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValuesRaw, JSONExtractKeysAndValuesRawImpl>>();

    factory.registerFunction<FunctionJSON<NameJSONHasByPtr, JSONHasImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONLengthByPtr, JSONLengthImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONKeyByPtr, JSONKeyImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONTypeByPtr, JSONTypeImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractIntByPtr, JSONExtractInt64Impl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractUIntByPtr, JSONExtractUInt64Impl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractFloatByPtr, JSONExtractFloat64Impl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractBoolByPtr, JSONExtractBoolImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractStringByPtr, JSONExtractStringImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractByPtr, JSONExtractImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValuesByPtr, JSONExtractKeysAndValuesImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractRawByPtr, JSONExtractRawImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractArrayRawByPtr, JSONExtractArrayRawImpl, /* by_ptr = */ true>>();
    factory.registerFunction<FunctionJSON<NameJSONExtractKeysAndValuesRawByPtr, JSONExtractKeysAndValuesRawImpl, /* by_ptr = */ true>>();
}

}
