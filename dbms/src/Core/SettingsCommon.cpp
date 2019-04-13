#include "SettingsCommon.h"

#include <Core/Field.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/FieldVisitors.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Common/SettingsChanges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int TYPE_MISMATCH;
    extern const int UNKNOWN_LOAD_BALANCING;
    extern const int UNKNOWN_OVERFLOW_MODE;
    extern const int ILLEGAL_OVERFLOW_MODE;
    extern const int UNKNOWN_TOTALS_MODE;
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int UNKNOWN_DISTRIBUTED_PRODUCT_MODE;
    extern const int UNKNOWN_GLOBAL_SUBQUERIES_METHOD;
    extern const int UNKNOWN_JOIN_STRICTNESS;
    extern const int UNKNOWN_LOG_LEVEL;
    extern const int SIZE_OF_FIXED_STRING_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

template <typename T>
String SettingNumber<T>::toString() const
{
    return DB::toString(value);
}

template <typename T>
Field SettingNumber<T>::toField() const
{
    return value;
}

template <typename T>
void SettingNumber<T>::set(T x)
{
    value = x;
    changed = true;
}

template <typename T>
void SettingNumber<T>::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(x.get<String>());
    else
        set(applyVisitor(FieldVisitorConvertToNumber<T>(), x));
}

template <typename T>
void SettingNumber<T>::set(const String & x)
{
    set(parse<T>(x));
}

template struct SettingNumber<UInt64>;
template struct SettingNumber<Int64>;
template struct SettingNumber<float>;


String SettingMaxThreads::toString() const
{
    /// Instead of the `auto` value, we output the actual value to make it easier to see.
    return DB::toString(value);
}

Field SettingMaxThreads::toField() const
{
    return is_auto ? 0 : value;
}

void SettingMaxThreads::set(UInt64 x)
{
    value = x ? x : getAutoValue();
    is_auto = x == 0;
    changed = true;
}

void SettingMaxThreads::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(x.get<String>());
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

void SettingMaxThreads::set(const String & x)
{
    if (x == "auto")
        setAuto();
    else
        set(parse<UInt64>(x));
}

void SettingMaxThreads::setAuto()
{
    value = getAutoValue();
    is_auto = true;
}

UInt64 SettingMaxThreads::getAutoValue() const
{
    static auto res = getAutoValueImpl();
    return res;
}

/// Executed once for all time. Executed from one thread.
UInt64 SettingMaxThreads::getAutoValueImpl() const
{
    return getNumberOfPhysicalCPUCores();
}


String SettingSeconds::toString() const
{
    return DB::toString(totalSeconds());
}

Field SettingSeconds::toField() const
{
    return static_cast<UInt64>(value.totalSeconds());
}

void SettingSeconds::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

void SettingSeconds::set(UInt64 x)
{
    set(Poco::Timespan(x, 0));
}

void SettingSeconds::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(x.get<String>());
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

void SettingSeconds::set(const String & x)
{
    set(parse<UInt64>(x));
}


String SettingMilliseconds::toString() const
{
    return DB::toString(totalMilliseconds());
}

Field SettingMilliseconds::toField() const
{
    return static_cast<UInt64>(value.totalMilliseconds());
}

void SettingMilliseconds::set(const Poco::Timespan & x)
{
    value = x;
    changed = true;
}

void SettingMilliseconds::set(UInt64 x)
{
    set(Poco::Timespan(x * 1000));
}

void SettingMilliseconds::set(const Field & x)
{
    if (x.getType() == Field::Types::String)
        set(x.get<String>());
    else
        set(applyVisitor(FieldVisitorConvertToNumber<UInt64>(), x));
}

void SettingMilliseconds::set(const String & x)
{
    set(parse<UInt64>(x));
}


LoadBalancing SettingLoadBalancing::getLoadBalancing(const String & s)
{
    if (s == "random")           return LoadBalancing::RANDOM;
    if (s == "nearest_hostname") return LoadBalancing::NEAREST_HOSTNAME;
    if (s == "in_order")         return LoadBalancing::IN_ORDER;

    throw Exception("Unknown load balancing mode: '" + s + "', must be one of 'random', 'nearest_hostname', 'in_order'",
        ErrorCodes::UNKNOWN_LOAD_BALANCING);
}

String SettingLoadBalancing::toString() const
{
    const char * strings[] = {"random", "nearest_hostname", "in_order"};
    if (value < LoadBalancing::RANDOM || value > LoadBalancing::IN_ORDER)
        throw Exception("Unknown load balancing mode", ErrorCodes::UNKNOWN_LOAD_BALANCING);
    return strings[static_cast<size_t>(value)];
}

Field SettingLoadBalancing::toField() const
{
    return toString();
}

void SettingLoadBalancing::set(LoadBalancing x)
{
    value = x;
    changed = true;
}

void SettingLoadBalancing::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingLoadBalancing::set(const String & x)
{
    set(getLoadBalancing(x));
}


JoinStrictness SettingJoinStrictness::getJoinStrictness(const String & s)
{
    if (s == "")       return JoinStrictness::Unspecified;
    if (s == "ALL")    return JoinStrictness::ALL;
    if (s == "ANY")    return JoinStrictness::ANY;

    throw Exception("Unknown join strictness mode: '" + s + "', must be one of '', 'ALL', 'ANY'",
        ErrorCodes::UNKNOWN_JOIN_STRICTNESS);
}

String SettingJoinStrictness::toString() const
{
    const char * strings[] = {"", "ALL", "ANY"};
    if (value < JoinStrictness::Unspecified || value > JoinStrictness::ANY)
        throw Exception("Unknown join strictness mode", ErrorCodes::UNKNOWN_JOIN_STRICTNESS);
    return strings[static_cast<size_t>(value)];
}

Field SettingJoinStrictness::toField() const
{
    return toString();
}

void SettingJoinStrictness::set(JoinStrictness x)
{
    value = x;
    changed = true;
}

void SettingJoinStrictness::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingJoinStrictness::set(const String & x)
{
    set(getJoinStrictness(x));
}


TotalsMode SettingTotalsMode::getTotalsMode(const String & s)
{
    if (s == "before_having")          return TotalsMode::BEFORE_HAVING;
    if (s == "after_having_exclusive") return TotalsMode::AFTER_HAVING_EXCLUSIVE;
    if (s == "after_having_inclusive") return TotalsMode::AFTER_HAVING_INCLUSIVE;
    if (s == "after_having_auto")      return TotalsMode::AFTER_HAVING_AUTO;

    throw Exception("Unknown totals mode: '" + s + "', must be one of 'before_having', 'after_having_exclusive', 'after_having_inclusive', 'after_having_auto'", ErrorCodes::UNKNOWN_TOTALS_MODE);
}

String SettingTotalsMode::toString() const
{
    switch (value)
    {
        case TotalsMode::BEFORE_HAVING:          return "before_having";
        case TotalsMode::AFTER_HAVING_EXCLUSIVE: return "after_having_exclusive";
        case TotalsMode::AFTER_HAVING_INCLUSIVE: return "after_having_inclusive";
        case TotalsMode::AFTER_HAVING_AUTO:      return "after_having_auto";
    }

    __builtin_unreachable();
}

Field SettingTotalsMode::toField() const
{
    return toString();
}

void SettingTotalsMode::set(TotalsMode x)
{
    value = x;
    changed = true;
}

void SettingTotalsMode::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingTotalsMode::set(const String & x)
{
    set(getTotalsMode(x));
}


template <bool enable_mode_any>
OverflowMode SettingOverflowMode<enable_mode_any>::getOverflowModeForGroupBy(const String & s)
{
    if (s == "throw") return OverflowMode::THROW;
    if (s == "break") return OverflowMode::BREAK;
    if (s == "any")   return OverflowMode::ANY;

    throw Exception("Unknown overflow mode: '" + s + "', must be one of 'throw', 'break', 'any'", ErrorCodes::UNKNOWN_OVERFLOW_MODE);
}

template <bool enable_mode_any>
OverflowMode SettingOverflowMode<enable_mode_any>::getOverflowMode(const String & s)
{
    OverflowMode mode = getOverflowModeForGroupBy(s);

    if (mode == OverflowMode::ANY && !enable_mode_any)
        throw Exception("Illegal overflow mode: 'any' is only for 'group_by_overflow_mode'", ErrorCodes::ILLEGAL_OVERFLOW_MODE);

    return mode;
}

template <bool enable_mode_any>
String SettingOverflowMode<enable_mode_any>::toString() const
{
    const char * strings[] = { "throw", "break", "any" };

    if (value < OverflowMode::THROW || value > OverflowMode::ANY)
        throw Exception("Unknown overflow mode", ErrorCodes::UNKNOWN_OVERFLOW_MODE);

    return strings[static_cast<size_t>(value)];
}

template <bool enable_mode_any>
Field SettingOverflowMode<enable_mode_any>::toField() const
{
    return toString();
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(OverflowMode x)
{
    value = x;
    changed = true;
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

template <bool enable_mode_any>
void SettingOverflowMode<enable_mode_any>::set(const String & x)
{
    set(getOverflowMode(x));
}

template struct SettingOverflowMode<false>;
template struct SettingOverflowMode<true>;

DistributedProductMode SettingDistributedProductMode::getDistributedProductMode(const String & s)
{
    if (s == "deny") return DistributedProductMode::DENY;
    if (s == "local") return DistributedProductMode::LOCAL;
    if (s == "global") return DistributedProductMode::GLOBAL;
    if (s == "allow") return DistributedProductMode::ALLOW;

    throw Exception("Unknown distributed product mode: '" + s + "', must be one of 'deny', 'local', 'global', 'allow'",
        ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
}

String SettingDistributedProductMode::toString() const
{
    const char * strings[] = {"deny", "local", "global", "allow"};
    if (value < DistributedProductMode::DENY || value > DistributedProductMode::ALLOW)
        throw Exception("Unknown distributed product mode", ErrorCodes::UNKNOWN_DISTRIBUTED_PRODUCT_MODE);
    return strings[static_cast<size_t>(value)];
}

Field SettingDistributedProductMode::toField() const
{
    return toString();
}

void SettingDistributedProductMode::set(DistributedProductMode x)
{
    value = x;
    changed = true;
}

void SettingDistributedProductMode::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingDistributedProductMode::set(const String & x)
{
    set(getDistributedProductMode(x));
}


String SettingString::toString() const
{
    return value;
}

Field SettingString::toField() const
{
    return value;
}

void SettingString::set(const String & x)
{
    value = x;
    changed = true;
}

void SettingString::set(const Field & x)
{
    set(safeGet<const String &>(x));
}


String SettingChar::toString() const
{
    return String(1, value);
}

Field SettingChar::toField() const
{
    return toString();
}

void SettingChar::set(char x)
{
    value = x;
    changed = true;
}

void SettingChar::set(const String & x)
{
    if (x.size() > 1)
        throw Exception("A setting's value string has to be an exactly one character long", ErrorCodes::SIZE_OF_FIXED_STRING_DOESNT_MATCH);
    char c = (x.size() == 1) ? x[0] : '\0';
    set(c);
}

void SettingChar::set(const Field & x)
{
    const String & s = safeGet<const String &>(x);
    set(s);
}


SettingDateTimeInputFormat::Value SettingDateTimeInputFormat::getValue(const String & s)
{
    if (s == "basic") return Value::Basic;
    if (s == "best_effort") return Value::BestEffort;

    throw Exception("Unknown DateTime input format: '" + s + "', must be one of 'basic', 'best_effort'", ErrorCodes::BAD_ARGUMENTS);
}

String SettingDateTimeInputFormat::toString() const
{
    const char * strings[] = {"basic", "best_effort"};
    if (value < Value::Basic || value > Value::BestEffort)
        throw Exception("Unknown DateTime input format", ErrorCodes::BAD_ARGUMENTS);
    return strings[static_cast<size_t>(value)];
}

Field SettingDateTimeInputFormat::toField() const
{
    return toString();
}

void SettingDateTimeInputFormat::set(Value x)
{
    value = x;
    changed = true;
}

void SettingDateTimeInputFormat::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingDateTimeInputFormat::set(const String & x)
{
    set(getValue(x));
}


SettingLogsLevel::Value SettingLogsLevel::getValue(const String & s)
{
    if (s == "none") return Value::none;
    if (s == "error") return Value::error;
    if (s == "warning") return Value::warning;
    if (s == "information") return Value::information;
    if (s == "debug") return Value::debug;
    if (s == "trace") return Value::trace;

    throw Exception("Unknown logs level: '" + s + "', must be one of: none, error, warning, information, debug, trace", ErrorCodes::BAD_ARGUMENTS);
}

String SettingLogsLevel::toString() const
{
    const char * strings[] = {"none", "error", "warning", "information", "debug", "trace"};
    return strings[static_cast<size_t>(value)];
}

Field SettingLogsLevel::toField() const
{
    return toString();
}

void SettingLogsLevel::set(Value x)
{
    value = x;
    changed = true;
}

void SettingLogsLevel::set(const Field & x)
{
    set(safeGet<const String &>(x));
}

void SettingLogsLevel::set(const String & x)
{
    set(getValue(x));
}



namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
}


struct SettingsBase
{
public:
    void setString(const String & name, const String & value) { (layout.getByName(name).set_string)(this, value); }
    void setField(const String & name, const Field & value) { (layout.getByName(name).set_field)(this, value); }
    void set(const String & name, const String & value) { setString(name, value); }
    void set(const String & name, const Field & value) { setField(name, value); }

    String getString(const String & name) const
    {
        String value;
        getString(name, value);
        return value;
    }

    Field getField(const String & name) const
    {
        String value;
        getString(name, value);
        return value;
    }

    void getString(const String & name, String & value) const { (layout.getByName(name).get_string)(this, value); }
    void getField(const String & name, Field & value) const { (layout.getByName(name).get_field)(this, value); }
    void get(const String & name, String & value) const { value = getString(name); }
    void get(const String & name, Field & value) const { value = getField(name); }

    bool tryGetString(const String & name, String & value) const
    {
        const auto * functions = layout.tryGetByName(name);
        if (!functions)
            return false;
        (functions->get_string)(this, value);
        return true;
    }

    bool tryGetField(const String & name, Field & value) const
    {
        const auto * functions = layout.tryGetByName(name);
        if (!functions)
            return false;
        (functions->get_field)(this, value);
        return true;
    }

    bool tryGet(const String & name, String & value) const { return tryGetString(name, value); }
    bool tryGet(const String & name, Field & value) const { return tryGetField(name, value); }

#if 0
    SettingsChanges changes() const
    {
        SettingsChanges collected_changes;
        for (const auto & entry : getLayout().getAll())
        {
            if (this->*maybe_changed_field.changed)
                collected_changes.push_back({maybe_changed_field.name.toString(), (maybe_changed_field.get_field)(this)});
        }
        return collected_changes;
    }
#endif

    void applyChange(const SettingChange & change)
    {
        (layout.getByName(change.getName()).set_field_unsafe)(this, change.getValue());
    }

    void applyChanges(const SettingsChanges & changes)
    {
        for (const SettingChange & change : changes)
            applyChange(change);
    }

protected:
    class Layout
    {
    public:
        typedef void (*GetStringFunction)(const SettingsBase*, String &);
        typedef void (*GetFieldFunction)(const SettingsBase*, Field &);
        typedef bool (*IsChangedFunction)(const SettingsBase*);
        typedef void (*SetStringFunction)(SettingsBase*, const String &);
        typedef void (*SetFieldFunction)(SettingsBase*, const Field &);
        typedef void (*SetFieldUnsafeFunction)(SettingsBase*, const Field &);

        struct Functions
        {
            GetStringFunction get_string;
            GetFieldFunction get_field;
            IsChangedFunction is_changed;
            SetStringFunction set_string;
            SetFieldFunction set_field;
            SetFieldUnsafeFunction set_field_unsafe;
        };

        const Functions & getByName(const String & name) const
        {
            const Functions * functions = tryGetByName(name);
            if (!functions)
                throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
            return *functions;
        }

        const Functions * tryGetByName(const String & name) const
        {
            auto it = functions_by_name.find(name);
            if (it == functions_by_name.end())
                return nullptr;
            return &it->second;
        }



        template <typename SettingType, SettingType SettingsBase::*SettingPtr>
        void add(StringRef name)
        {
            functions_by_name.emplace(name,
                                      {&FunctionsImpl<SettingType, SettingPtr>::getString,
                                       &FunctionsImpl<SettingType, SettingPtr>::getField,
                                       &FunctionsImpl<SettingType, SettingPtr>::isChanged,
                                       &FunctionsImpl<SettingType, SettingPtr>::setString,
                                       &FunctionsImpl<SettingType, SettingPtr>::setField,
                                       &FunctionsImpl<SettingType, SettingPtr>::setFieldUnsafe});

        }

    private:
        template <typename SettingType, SettingType SettingsBase::*SettingPtr>
        struct FunctionsImpl
        {
            static void getString(const SettingsBase* pSettings, String & value)
            {
                (pSettings->*SettingPtr)->getString(value);
            }

            static void getField(const SettingsBase* pSettings, Field & value)
            {
                (pSettings->*SettingPtr)->getField(value);
            }

            static bool isChanged(const SettingsBase* pSettings)
            {
                return (pSettings->*SettingPtr)->changed;
            }

            static void setString(SettingsBase* pSettings, const String & value)
            {
                (pSettings->*SettingPtr)->setString(value);
            }

            static void setField(SettingsBase* pSettings, const Field & value)
            {
                (pSettings->*SettingPtr)->setField(value);
            }

            static void setFieldUnsafe(SettingsBase* pSettings, const Field & value)
            {
                (pSettings->*SettingPtr)->setField(value);
            }
        };

        template <typename SettingType, SettingType SettingsBase::*SettingPtr>
        static size_t offsetToChanged()
        {
            SettingsBase * pSettings = reinterpret_cast<SettingsBase*>(1);
            bool & changed = (pSettings->*SettingPtr)->changed;
            return reinterpret_cast<const UInt8*>(&changed) - reinterpret_cast<const UInt8*>(pSettings);
        }

        std::unordered_map<StringRef, Functions> functions_by_name;
        using NameAndFunctions = std::pair<StringRef, Functions>;
        std::vector<std::pair<size_t, const NameAndFunctions*>> all_entries;
    };

    SettingsBase(const Layout & layout_) : layout(layout_) {}
    const Layout & layout;
};

}
