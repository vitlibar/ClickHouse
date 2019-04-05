#include <Core/SettingsChanges.h>

#include <Common/FieldVisitors.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

// We need to include the following 3 headers only for macros
// APPLY_FOR_SETTINGS, APPLY_FOR_KAFKA_SETTINGS, APPLY_FOR_MERGE_TREE_SETTINGS.
#include <Core/Settings.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>


namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_SETTING;
extern const int BAD_TYPE_OF_FIELD;
extern const int LOGICAL_ERROR;
}


namespace
{
    template<typename SettingType>
    struct UtilsForSettingType;


    template <typename T>
    struct UtilsForSettingType<SettingNumber<T>>
    {
        static Field stringToField(const String & s)
        {
            return parse<T>(s);
        }

        static Field adjustFieldType(const Field & field)
        {
            if (field.getType() == Field::Types::String)
                return stringToField(field.get<String>());
            else
                return applyVisitor(FieldVisitorConvertToNumber<T>(), field);
        }

        static Field deserialize(ReadBuffer & buf)
        {
            T value;
            if constexpr (std::is_integral_v<T>)
            {
                value = 0;
                readVarT(value, buf);
            }
            else
            {
                String s;
                readBinary(s, buf);
                value = parse<T>(s);
            }
            return value;
        }
    };


    template<>
    struct UtilsForSettingType<SettingMaxThreads>
    {
        static Field stringToField(const String & s)
        {
            if (s == "auto")
                return UInt64(0);
            else
                return parse<UInt64>(s);
        }

        static Field adjustFieldType(const Field & field)
        {
            if (field.getType() == Field::Types::String)
                return stringToField(field.get<String>());
            else
                return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), field);
        }

        static Field deserialize(ReadBuffer & buf)
        {
            UInt64 max_threads = 0;
            readVarUInt(max_threads, buf);
            return max_threads;
        }
    };


    template<>
    struct UtilsForSettingType<SettingSeconds> : public UtilsForSettingType<SettingNumber<UInt64>> {};

    template<>
    struct UtilsForSettingType<SettingMilliseconds> : public UtilsForSettingType<SettingNumber<UInt64>> {};


    template<>
    struct UtilsForSettingType<SettingString>
    {
        static Field stringToField(const String & s)
        {
            return s;
        }

        static Field adjustFieldType(const Field & field)
        {
            if (field.getType() != Field::Types::String)
                throw Exception("The setting can be assigned only to a string, not to " + String(field.getTypeName()), ErrorCodes::BAD_TYPE_OF_FIELD);
            return field;
        }

        static Field deserialize(ReadBuffer & buf)
        {
            String s;
            readBinary(s, buf);
            return s;
        }
    };


    template<>
    struct UtilsForSettingType<SettingChar> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingLoadBalancing> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingJoinStrictness> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingTotalsMode> : public UtilsForSettingType<SettingString> {};

    template <bool enable_mode_any>
    struct UtilsForSettingType<SettingOverflowMode<enable_mode_any>> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingDistributedProductMode> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingDateTimeInputFormat> : public UtilsForSettingType<SettingString> {};

    template <>
    struct UtilsForSettingType<SettingLogsLevel> : public UtilsForSettingType<SettingString> {};


    class Dispatcher : public ext::singleton<Dispatcher>
    {
    public:
        Dispatcher()
        {
            std::unordered_map<StringRef, Functions>::iterator it;
            bool inserted;
            Functions functions;
#define ADD_FUNCTIONS(TYPE, NAME, DEFAULT, DESCRIPTION) \
                functions = Functions{ &UtilsForSettingType<TYPE>::stringToField, \
                                       &UtilsForSettingType<TYPE>::adjustFieldType, \
                                       &UtilsForSettingType<TYPE>::deserialize }; \
                std::tie(it, inserted) = map.emplace(StringRef(#NAME, strlen(#NAME)), functions); \
                if (!inserted && (it->second != functions)) \
                    throw Exception("Two settings named '" #NAME "' have different types", ErrorCodes::LOGICAL_ERROR);

            APPLY_FOR_SETTINGS(ADD_FUNCTIONS)
            APPLY_FOR_KAFKA_SETTINGS(ADD_FUNCTIONS)
            APPLY_FOR_MERGE_TREE_SETTINGS(ADD_FUNCTIONS)

            // The class SettingsChanges is also used to store the profile name which isn't one of the Settings.
            ADD_FUNCTIONS(SettingString, profile, "", "")
#undef ADD_FUNCTIONS
        }

        Field stringToField(const String & value, const String & name) const
        {
            const StringToFieldFunction string_to_field = getFunctions(name).string_to_field;
            try
            {
                return string_to_field(value);
            }
            catch (Exception & e)
            {
                e.addMessage("setting " + name);
                throw;
            }
        }

        Field adjustFieldType(const Field & value, const String & name) const
        {
            const AdjustFieldTypeFunction adjust_field_type = getFunctions(name).adjust_field_type;
            try
            {
                return adjust_field_type(value);
            }
            catch (Exception & e)
            {
                e.addMessage("setting " + name);
                throw;
            }
        }

        Field deserialize(ReadBuffer & buf, const String & name) const
        {
            return getFunctions(name).deserialize(buf);
        }

    private:
        typedef Field (*StringToFieldFunction)(const String &);
        typedef Field (*AdjustFieldTypeFunction)(const Field &);
        typedef Field (*DeserializeFunction)(ReadBuffer &);
        struct Functions
        {
            StringToFieldFunction string_to_field;
            AdjustFieldTypeFunction adjust_field_type;
            DeserializeFunction deserialize;
            bool operator==(const Functions & other) const
            {
                return string_to_field == other.string_to_field && adjust_field_type == other.adjust_field_type
                    && deserialize == other.deserialize;
            }
            bool operator!=(const Functions & other) const { return !(*this == other); }
        };

        const Functions & getFunctions(const String & name) const
        {
            auto it = map.find(name);
            if (it == map.end())
                throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
            return it->second;
        }

        std::unordered_map<StringRef, Functions> map;
    };
}


SettingChange::SettingChange(const String & name_, const String & value_)
    : name(name_), value(Dispatcher::instance().stringToField(value_, name_)) {}

SettingChange::SettingChange(const String & name_, const Field & value_)
    : name(name_), value(adjustFieldTypeForSetting(value_, name_)) {}

SettingChange::SettingChange(const String & name_, ReadBuffer & buf)
    : name(name_), value(Dispatcher::instance().deserialize(buf, name_)) {}

void SettingChange::serializeValue(WriteBuffer & buf) const
{
    switch (value.getType())
    {
        case Field::Types::Int64: writeVarInt(value.get<Int64>(), buf); break;
        case Field::Types::UInt64: writeVarUInt(value.get<UInt64>(), buf); break;
        case Field::Types::Float64: writeBinary(toString(value.get<Float64>()), buf); break;
        case Field::Types::String: writeBinary(value.get<String>(), buf); break;
        default: throw Exception("Field containing value of setting " + name + " has unexpected type " + value.getTypeName(), ErrorCodes::BAD_TYPE_OF_FIELD);
    }
}


Field SettingChange::adjustFieldTypeForSetting(const Field & field, const String & name)
{
    return Dispatcher::instance().adjustFieldType(field, name);
}


void SettingsChanges::deserialize(ReadBuffer & buf)
{
    clear();
    while (true)
    {
        String name;
        readBinary(name, buf);

        /// An empty string is the marker for the end of the settings.
        if (name.empty())
            break;

        push_back({name, buf});
    }
}

void SettingsChanges::serialize(WriteBuffer & buf) const
{
    for (const SettingChange & change : changes)
    {
        writeStringBinary(change.getName(), buf);
        change.serializeValue(buf);
    }

    /// An empty string is a marker for the end of the settings.
    writeStringBinary("", buf);
}

}
