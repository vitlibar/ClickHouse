#pragma once

#include <Core/Field.h>


namespace DB
{

class SettingChange
{
public:
    SettingChange(const String & name_, const String & value_);
    SettingChange(const String & name_, const Field & value_);
    SettingChange(const String & name_, ReadBuffer & buf);

    const String & getName() const { return name; }
    const Field & getValue() const { return value; }

    void serializeValue(WriteBuffer & buf) const;

    static Field adjustFieldTypeForSetting(const Field & field, const String & name);

private:
    String name;
    Field value;
};


/** Keeps changes of settings, contains a vector of SettingChange and supports serialization.
  */
class SettingsChanges
{
    using Container = std::vector<SettingChange>;
public:
    using const_iterator = Container::const_iterator;

    SettingsChanges() {}

    /// Vector-like access to elements.
    size_t size() const { return changes.size(); }
    bool empty() const { return changes.empty(); }
    void clear() { changes.clear(); }

    const SettingChange & operator[](size_t i) const { return changes[i]; }
    const_iterator begin() const { return changes.begin(); }
    const_iterator end() const { return changes.end(); }
    const SettingChange * data() const { return changes.data(); }

    void push_back(const SettingChange & change) { changes.emplace_back(change); }
    void push_back(SettingChange && change) { changes.emplace_back(change); }
    const SettingChange & back() const { return changes.back(); }

    /// Writes changes to buffer. They are serialized as list of contiguous name-value pairs, finished with empty name.
    void serialize(WriteBuffer & buf) const;

    /// Reads changes from buffer.
    void deserialize(ReadBuffer & buf);

private:
    Container changes;
};

}
