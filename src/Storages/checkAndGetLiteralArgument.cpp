#include <Storages/checkAndGetLiteralArgument.h>
#include <Core/Field.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

template <typename T>
T checkAndGetLiteralArgument(const ASTPtr & arg, const String & arg_name)
{
    if (arg && arg->as<ASTLiteral>())
        return checkAndGetLiteralArgument<T>(*arg->as<ASTLiteral>(), arg_name);

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument '{}' must be a literal, get {} (value: {})",
        arg_name,
        arg ? arg->getID() : "NULL",
        arg ? arg->formatForErrorMessage() : "NULL");
}

template <typename T>
T checkAndGetLiteralArgument(const ASTLiteral & arg, const String & arg_name)
{
    /// tryGetAsBool() is used to allow using 0 or 1 as booleans too.
    if (arg.value.tryGetAs<T>(value))
        return value;

    T value;
    if constexpr (std::is_same_v<T, bool>)
    {
    }
    else
    {
        if (arg.value.tryGet(value))
            return value;
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Argument '{}' must be a literal with type {}, got {}",
        arg_name,
        fieldTypeToString(Field::TypeToEnum<NearestFieldType<T>>::value),
        fieldTypeToString(arg.value.getType()));
}

template String checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template UInt8 checkAndGetLiteralArgument(const ASTPtr &, const String &);
template bool checkAndGetLiteralArgument(const ASTPtr &, const String &);
template String checkAndGetLiteralArgument(const ASTLiteral &, const String &);
template UInt64 checkAndGetLiteralArgument(const ASTLiteral &, const String &);
}
