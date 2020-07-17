#pragma once

#include <Core/SettingsFields.h>
#include <Common/SettingsChanges.h>
#include <Common/FieldVisitors.h>
#include <ext/range.h>
#include <unordered_map>


namespace DB
{
class ReadBuffer;
class WriteBuffer;

enum class SettingsWriteFormat
{
    BINARY,             /// Part of the settings are serialized as strings, and other part as variants. This is the old behaviour.
    STRINGS_WITH_FLAGS, /// All settings are serialized as strings. Before each value the flag `is_important` is serialized.
    DEFAULT = STRINGS_WITH_FLAGS,
};


/** Template class to define collections of settings.
  * Example of usage:
  *
  * mysettings.h:
  * #define APPLY_FOR_MYSETTINGS(M) \
  *     M(UInt64, a, 100, "Description of a", 0) \
  *     M(Float, f, 3.11, "Description of f", IMPORTANT) // IMPORTANT - means the setting can't be ignored by older versions) \
  *     M(String, s, "default", "Description of s", 0)
  *
  * DECLARE_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS)

  * struct MySettings : public BaseSettings<MySettingsTraits>
  * {
  * };
  *
  * mysettings.cpp:
  * IMPLEMENT_SETTINGS_TRAITS(MySettingsTraits, APPLY_FOR_MYSETTINGS)
  */
template <class Traits_>
class BaseSettings : public Traits_::Data
{
public:
    using Traits = Traits_;

    void set(const std::string_view & name, const Field & value);
    void set(const std::string_view & name, const String & value);

    Field get(const std::string_view & name) const;
    String getAsString(const std::string_view & name) const;

    bool tryGet(const std::string_view & name, Field & value) const;
    bool tryGet(const std::string_view & name, String & value) const;

    bool isChanged(const std::string_view & name) const;
    static std::string_view getDescription(const std::string_view & name);

    SettingsChanges changes() const;
    void applyChanges(const SettingsChanges & changes);
    void applyChanges(const BaseSettings & changes);

    void resetToDefault();

    // A debugging aid.
    std::string toString() const;

    void write(WriteBuffer & out, SettingsWriteFormat format) const;
    void read(ReadBuffer & in, SettingsWriteFormat format);

    class SettingFieldRef
    {
    public:
        SettingFieldRef(const typename Traits::Data & data_, size_t index_) : data(&data_), index(index_) {}

        const String & getName() const { return Traits::getName(index); }
        Field getValue() const { return Traits::getValue(*data, index); }
        String getValueAsString() const { return Traits::getValueAsString(*data, index); }
        bool isChanged() const { return Traits::isChanged(*data, index); }
        std::string_view getType() const { return Traits::getType(index); }
        std::string_view getDescription() const { return Traits::getName(index); }

    private:
        friend class BaseSettings<Traits>;
        const typename Traits::Data * data;
        size_t index;
    };

    class Iterator
    {
    public:
        Iterator(const typename Traits::Data & data_, size_t index_, bool skip_unchanged_);
        Iterator & operator++();
        Iterator operator++(int);
        SettingFieldRef operator *() const { return {*data, index}; }

        friend bool operator ==(const Iterator & left, const Iterator & right) { return (left.index == right.index) && (left.data == right.data); }
        friend bool operator !=(const Iterator & left, const Iterator & right) { return !(left == right); }

    private:
        void doSkipUnchanged();

        const typename Traits::Data * data;
        size_t index;
        bool skip_unchanged;
    };

    Iterator begin(bool with_unchanged = false) const { return Iterator(*this, 0, !with_unchanged); }
    Iterator end() const { return Iterator(*this, 0, Traits::size()); }

    class AllIncludingUnchanged
    {
    public:
        AllIncludingUnchanged(const BaseSettings<Traits> & settings_) : settings(settings_) {}
        Iterator begin() const { return settings.begin(true); }
        Iterator end() const { return settings.end(); }

    private:
        const BaseSettings<Traits> & settings;
    };

    AllIncludingUnchanged allIncludingUnchanged() const { return AllIncludingUnchanged{*this}; }

private:
    static size_t getIndex(const std::string_view & name);
};

struct BaseSettingsHelper
{
    static void writeString(const std::string_view & str, WriteBuffer & out);
    static String readString(ReadBuffer & in);
    static void writeFlag(bool flag, WriteBuffer & out);
    static bool readFlag(ReadBuffer & in);
    static void throwSettingNotFound(const std::string_view & name);
    static void warningSettingNotFound(const std::string_view & name);
};

template <typename Traits_>
void BaseSettings<Traits_>::set(const std::string_view & name, const Field & value)
{
    Traits::setValue(*this, getIndex(name), value);
}

template <typename Traits_>
void BaseSettings<Traits_>::set(const std::string_view & name, const String & value)
{
    Traits::setValue(*this, getIndex(name), value);
}

template <typename Traits_>
Field BaseSettings<Traits_>::get(const std::string_view & name) const
{
    return Traits::getValue(*this, getIndex(name));
}

template <typename Traits_>
String BaseSettings<Traits_>::getAsString(const std::string_view & name) const
{
    return Traits::getValueAsString(*this, getIndex(name));
}

template <typename Traits_>
size_t BaseSettings<Traits_>::getIndex(const std::string_view & name)
{
    size_t index = Traits::find(name);
    if (index == static_cast<size_t>(-1))
        BaseSettingsHelper::throwSettingNotFound(name);
    return index;
}

template <typename Traits_>
bool BaseSettings<Traits_>::tryGet(const std::string_view & name, Field & value) const
{
    size_t index = Traits::find(name);
    if (index == static_cast<size_t>(-1))
        return false;
    value = Traits::getValue(*this, index);
    return true;
}

template <typename Traits_>
bool BaseSettings<Traits_>::tryGet(const std::string_view & name, String & value) const
{
    size_t index = Traits::find(name);
    if (index == static_cast<size_t>(-1))
        return false;
    value = Traits::getValue(*this, index);
    return true;
}

template <typename Traits_>
bool BaseSettings<Traits_>::isChanged(const std::string_view & name) const
{
    size_t index = Traits::find(name);
    if (index == static_cast<size_t>(-1))
        return false;
    return Traits::isChanged(*this, index);
}

template <typename Traits_>
std::string_view BaseSettings<Traits_>::getDescription(const std::string_view & name)
{
    size_t index = Traits::find(name);
    if (index == static_cast<size_t>(-1))
        return {};
    return Traits::getDescription(index);
}

template <typename Traits_>
BaseSettings<Traits_>::Iterator::Iterator(const typename Traits::Data & data_, size_t index_, bool skip_unchanged_)
    : data(&data_), index(index_), skip_unchanged(skip_unchanged_)
{
    doSkipUnchanged();
}

template <typename Traits_>
typename BaseSettings<Traits_>::Iterator & BaseSettings<Traits_>::Iterator::operator++()
{
    ++index;
    doSkipUnchanged();
    return *this;
}

template <typename Traits_>
typename BaseSettings<Traits_>::Iterator BaseSettings<Traits_>::Iterator::operator++(int)
{
    auto res = *this;
    ++*this;
    return res;
}

template <typename Traits_>
void BaseSettings<Traits_>::Iterator::doSkipUnchanged()
{
    if (!skip_unchanged)
        return;
    for (; index != Traits::size(); ++index)
    {
        if (Traits::isValueChanged(*data, index))
            break;
    }
}

template <typename Traits_>
SettingsChanges BaseSettings<Traits_>::changes() const
{
    SettingsChanges res;
    for (const auto & [name, value] : *this)
        res.emplace_back(name, value);
    return res;
}

template <typename Traits_>
void BaseSettings<Traits_>::applyChanges(const SettingsChanges & changes)
{
    for (const auto & change : changes)
        set(change.name, change.value);
}

template <typename Traits_>
void BaseSettings<Traits_>::applyChanges(const BaseSettings & changes)
{
    for (const auto & [name, value] : changes)
        set(name, value);
}

template <typename Traits_>
void BaseSettings<Traits_>::resetToDefault()
{
    for (size_t i : ext::range(Traits::size()))
    {
        if (Traits::isValueChanged(*this, i))
            Traits::resetValueToDefault(*this, i);
    }
}


template <typename Traits_>
String BaseSettings<Traits_>::toString() const
{
    String res;
    for (const auto & [name, value] : *this)
    {
        if (!res.empty())
            res += ", ";
        res += name + " = " + applyVisitor(FieldVisitorToString(), value);
    }
    return res;
}

template <typename Traits_>
void BaseSettings<Traits_>::write(WriteBuffer & out, SettingsWriteFormat format) const
{
    for (auto field : *this)
    {
        BaseSettingsHelper::writeString(field.getName(), out);
        if (format >= SettingsWriteFormat::STRINGS_WITH_FLAGS)
        {
            BaseSettingsHelper::writeFlag(Traits::isImportant(field.index), out);
            BaseSettingsHelper::writeString(Traits::getValueAsString(*this, field.index), out);
        }
        else
            Traits::writeBinary(*this, field.index, out);
    }

    /// Empty string is a marker of the end of settings.
    BaseSettingsHelper::writeString(std::string_view{}, out);
}

template <typename Traits_>
void BaseSettings<Traits_>::read(ReadBuffer & in, SettingsWriteFormat format)
{
    resetToDefault();
    while (true)
    {
        String name = BaseSettingsHelper::readString(in);
        if (name.empty() /* empty string is a marker of the end of settings */)
            break;
        size_t index = Traits::find(name);

        bool is_important = true;
        if (format >= SettingsWriteFormat::STRINGS_WITH_FLAGS)
            is_important = BaseSettingsHelper::readFlag(in);

        if (index == static_cast<size_t>(-1))
        {
            if (is_important)
                BaseSettingsHelper::throwSettingNotFound(name);
            else
            {
                BaseSettingsHelper::warningSettingNotFound(name);
                BaseSettingsHelper::readString(in);
                continue;
            }
        }

        if (format >= SettingsWriteFormat::STRINGS_WITH_FLAGS)
            Traits::setValue(*this, index, BaseSettingsHelper::readString(in));
        else
            Traits::readBinary(*this, index, in);
    }
}

#define DECLARE_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO) \
    struct SETTINGS_TRAITS_NAME \
    { \
        struct Data \
        { \
            LIST_OF_SETTINGS_MACRO(DECLARE_SETTINGS_TRAITS_) \
        }; \
        \
        static size_t size(); \
        static size_t find(const std::string_view & name); \
        static const String & getName(size_t index); \
        static std::string_view getType(size_t index); \
        static std::string_view getDescription(size_t index); \
        static bool isImportant(size_t index); \
        static Field convertValue(size_t index, const Field & value); \
        static String convertValueToString(size_t index, const Field & value); \
        static Field getValue(const Data & data, size_t index); \
        static String getValueAsString(const Data & data, size_t index); \
        static void setValue(Data & data, size_t index, const Field & value); \
        static void setValue(Data & data, size_t index, const String & value); \
        static bool isValueChanged(const Data & data, size_t index); \
        static void resetValueToDefault(Data & data, size_t index); \
        static void writeBinary(const Data & data, size_t index, WriteBuffer & out); \
        static void readBinary(Data & data, size_t index, ReadBuffer & in); \
    };

#define DECLARE_SETTINGS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    SettingField##TYPE NAME {DEFAULT};

#define IMPLEMENT_SETTINGS_TRAITS(SETTINGS_TRAITS_NAME, LIST_OF_SETTINGS_MACRO) \
    struct SETTINGS_TRAITS_NAME##Info \
    { \
        static const SETTINGS_TRAITS_NAME##Info & instance() \
        { \
            static SETTINGS_TRAITS_NAME##Info the_instance = [] \
            { \
                SETTINGS_TRAITS_NAME##Info res; \
                constexpr int IMPORTANT = 1; \
                UNUSED(IMPORTANT); \
                LIST_OF_SETTINGS_MACRO(IMPLEMENT_SETTINGS_TRAITS_) \
                for (size_t i : ext::range(res.field_infos.size())) \
                { \
                    const auto & info = res.field_infos[i]; \
                    res.name_to_index_map.emplace(info.name, i); \
                } \
                return res; \
            }(); \
            return the_instance; \
        } \
        \
        using Data = SETTINGS_TRAITS_NAME::Data; \
        using ConvertValueFunction = Field (*)(const Field &); \
        using ConvertValueToStringFunction = String (*)(const Field &); \
        using GetValueFunction = Field (*)(const Data &); \
        using GetValueAsStringFunction = String (*)(const Data &); \
        using SetValueFunction = void (*)(Data &, const Field &); \
        using SetValueAsStringFunction = void (*)(Data &, const String &); \
        using IsValueChangedFunction = bool (*)(const Data &); \
        using ResetValueToDefaultFunction = void (*)(Data &); \
        using WriteBinaryFunction = void (*)(const Data &, WriteBuffer &); \
        using ReadBinaryFunction = void (*)(Data &, ReadBuffer &); \
        \
        struct FieldInfo \
        { \
            String name; \
            std::string_view type; \
            std::string_view description; \
            bool is_important; \
            ConvertValueFunction convert_value_function; \
            ConvertValueToStringFunction convert_value_to_string_function; \
            GetValueFunction get_value_function; \
            GetValueAsStringFunction get_value_as_string_function; \
            SetValueFunction set_value_function; \
            SetValueAsStringFunction set_value_as_string_function; \
            IsValueChangedFunction is_value_changed_function; \
            ResetValueToDefaultFunction reset_value_to_default_function; \
            WriteBinaryFunction write_binary_function; \
            ReadBinaryFunction read_binary_function; \
        }; \
        \
        std::vector<FieldInfo> field_infos; \
        std::unordered_map<std::string_view, size_t> name_to_index_map; \
    }; \
    \
    size_t SETTINGS_TRAITS_NAME::size() { return SETTINGS_TRAITS_NAME##Info::instance().size(); } \
    static size_t SETTINGS_TRAITS_NAME::find(const std::string_view & name) { return SETTINGS_TRAITS_NAME##Info::instance().find(name); } \
    static const String & SETTINGS_TRAITS_NAME::getName(size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].name; } \
    static std::string_view SETTINGS_TRAITS_NAME::getType(size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].type; } \
    static std::string_view SETTINGS_TRAITS_NAME::getDescription(size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].description; } \
    static bool SETTINGS_TRAITS_NAME::isImportant(size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].is_important; } \
    static Field SETTINGS_TRAITS_NAME::convertValue(size_t index, const Field & value) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].convert_value_function(value); } \
    static String SETTINGS_TRAITS_NAME::convertValueToString(size_t index, const Field & value) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].convert_value_to_string_function(value); } \
    static Field SETTINGS_TRAITS_NAME::getValue(const Data & data, size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].get_value_function(data); } \
    static String SETTINGS_TRAITS_NAME::getValueAsString(const Data & data, size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].get_value_as_string_function(data); } \
    static void SETTINGS_TRAITS_NAME::setValue(Data & data, size_t index, const Field & value) { SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].set_value_function(data, value); } \
    static void SETTINGS_TRAITS_NAME::setValue(Data & data, size_t index, const String & value) { SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].set_value_as_string_function(data, value); } \
    static bool SETTINGS_TRAITS_NAME::isValueChanged(const Data & data, size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].is_value_changed_function(data); } \
    static void SETTINGS_TRAITS_NAME::resetValueToDefault(Data & data, size_t index) { SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].reset_value_to_default_function(data); } \
    static void SETTINGS_TRAITS_NAME::writeBinary(const Data & data, size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].write_binary_function(data, out); } \
    static void SETTINGS_TRAITS_NAME::readBinary(Data & data, size_t index) { return SETTINGS_TRAITS_NAME##Info::instance().field_infos[index].read_binary_function(data, in); } \
    \
    template class BaseSettings<SETTINGS_TRAITS_NAME>;

//-V:IMPLEMENT_SETTINGS:501
#define IMPLEMENT_SETTINGS_TRAITS_(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) \
    res.field_infos.emplace_back( \
        FieldInfo{#NAME, #TYPE, #DESCRIPTION, FLAGS & IMPORTANT, \
            [](const Field & value) -> Field { SettingField##TYPE tmp; tmp.set(value); return tmp.toField(); }, \
            [](const Field & value) -> String { SettingField##TYPE tmp; tmp.set(value); return tmp.toString(); }, \
            [](const Data & data) -> Field { return data.Name.toField(); }, \
            [](const Data & data) -> String { return data.Name.toString(); }, \
            [](Data & data, const Field & value) { data.Name.set(value); }, \
            [](Data & data, const String & value) { data.Name.set(value); }, \
            [](const Data & data) -> bool { return data.NAME.changed; }, \
            [](Data & data) { data.Name = TYPE{DEFAULT}; }, \
            [](const Data & data, WriteBuffer & out) { data.Name.writeBinary(out); }, \
            [](Data & data, ReadBuffer & out) { data.Name.readBinary(out); } \
        });
}
