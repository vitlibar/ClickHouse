#pragma once

#include <Core/Field.h>
#include <Core/UUID.h>
#include <Common/SettingConstraintWritability.h>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{
struct Settings;
class SettingsChanges;
class SettingsConstraints;
struct AlterSettingsProfileElements;
class ASTSettingsProfileElement;
class ASTSettingsProfileElements;
class ASTAlterSettingsProfileElements;
class AccessControl;


struct SettingsProfileElement
{
    std::optional<UUID> parent_profile;

    String setting_name;
    std::optional<Field> value;
    std::optional<Field> min_value;
    std::optional<Field> max_value;
    std::optional<SettingConstraintWritability> writability;

    auto toTuple() const { return std::tie(parent_profile, setting_name, value, min_value, max_value, writability); }
    friend bool operator==(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() == rhs.toTuple(); }
    friend bool operator!=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs == rhs); }
    friend bool operator <(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return lhs.toTuple() < rhs.toTuple(); }
    friend bool operator >(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return rhs < lhs; }
    friend bool operator <=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(rhs < lhs); }
    friend bool operator >=(const SettingsProfileElement & lhs, const SettingsProfileElement & rhs) { return !(lhs < rhs); }

    SettingsProfileElement() = default;

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    SettingsProfileElement(const ASTSettingsProfileElement & ast); /// NOLINT
    SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControl & access_control);
    std::shared_ptr<ASTSettingsProfileElement> toAST() const;
    std::shared_ptr<ASTSettingsProfileElement> toASTWithNames(const AccessControl & access_control) const;

    bool empty() const { return !parent_profile && (setting_name.empty() || (!value && !min_value && !max_value && !writability)); }

    bool isConstraint() const;

private:
    void init(const ASTSettingsProfileElement & ast, const AccessControl * access_control);
};


class SettingsProfileElements : public std::vector<SettingsProfileElement>
{
public:
    SettingsProfileElements() = default;

    /// The constructor from AST requires the AccessControl if `ast.id_mode == false`.
    SettingsProfileElements(const ASTSettingsProfileElements & ast); /// NOLINT
    SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control);
    std::shared_ptr<ASTSettingsProfileElements> toAST() const;
    std::shared_ptr<ASTSettingsProfileElements> toASTWithNames(const AccessControl & access_control) const;

    std::vector<UUID> findDependencies() const;
    void replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids);

    void merge(const SettingsProfileElements & other);

    void applyChanges(const AlterSettingsProfileElements & changes);

    Settings toSettings() const;
    SettingsChanges toSettingsChanges() const;
    SettingsConstraints toSettingsConstraints(const AccessControl & access_control) const;
    std::vector<UUID> toProfileIDs() const;

    bool isBackupAllowed() const;

    static bool isAllowBackupSetting(const String & setting_name);
};

struct AlterSettingsProfileElements
{
    bool drop_all_settings = false;
    bool drop_all_profiles = false;
    SettingsProfileElements add_settings;
    SettingsProfileElements modify_settings;
    SettingsProfileElements drop_settings;

    AlterSettingsProfileElements() = default;
    explicit AlterSettingsProfileElements(const SettingsProfileElements & ast);
    explicit AlterSettingsProfileElements(const ASTSettingsProfileElements & ast);
    explicit AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast);
    AlterSettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control);
    AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast, const AccessControl & access_control);
};

}
