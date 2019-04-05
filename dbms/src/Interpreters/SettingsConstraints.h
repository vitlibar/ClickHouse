#pragma once


namespace DB
{
class SettingChange;
class SettingsChanges;
struct Settings;


class SettingsConstraints
{
public:
    static void check(const Settings & settings, const SettingChange & change);
    static void check(const Settings & settings, const SettingsChanges & changes);
};

}
