#include <Access/SettingsProfileElement.h>
#include <Access/SettingsConstraints.h>
#include <Access/AccessControl.h>
#include <Access/SettingsProfile.h>
#include <Core/Settings.h>
#include <Common/SettingConstraintWritability.h>
#include <Common/SettingsChanges.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Parsers/Access/ASTSettingsProfileElement.h>
#include <base/removeDuplicates.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast)
{
    init(ast, nullptr);
}

SettingsProfileElement::SettingsProfileElement(const ASTSettingsProfileElement & ast, const AccessControl & access_control)
{
    init(ast, &access_control);
}

void SettingsProfileElement::init(const ASTSettingsProfileElement & ast, const AccessControl * access_control)
{
    auto name_to_id = [id_mode{ast.id_mode}, access_control](const String & name_) -> UUID
    {
        if (id_mode)
            return parse<UUID>(name_);
        assert(access_control);
        return access_control->getID<SettingsProfile>(name_);
    };

    if (!ast.parent_profile.empty())
        parent_profile = name_to_id(ast.parent_profile);

    if (!ast.setting_name.empty())
    {
        setting_name = ast.setting_name;

        if (access_control)
        {
            /// Check if a setting with that name is allowed.
            if (!SettingsProfileElements::isAllowBackupSetting(setting_name))
                access_control->checkSettingNameIsAllowed(setting_name);
            /// Check if a CHANGEABLE_IN_READONLY is allowed.
            if (ast.writability == SettingConstraintWritability::CHANGEABLE_IN_READONLY && !access_control->doesSettingsConstraintsReplacePrevious())
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "CHANGEABLE_IN_READONLY for {} "
                                "is not allowed unless settings_constraints_replace_previous is enabled", setting_name);
        }

        value = ast.value;
        min_value = ast.min_value;
        max_value = ast.max_value;
        writability = ast.writability;

        if (value)
            value = Settings::castValueUtil(setting_name, *value);
        if (min_value)
            min_value = Settings::castValueUtil(setting_name, *min_value);
        if (max_value)
            max_value = Settings::castValueUtil(setting_name, *max_value);
    }
}

bool SettingsProfileElement::isConstraint() const
{
    return this->writability || this->min_value || this->max_value;
}

std::shared_ptr<ASTSettingsProfileElement> SettingsProfileElement::toAST() const
{
    auto ast = std::make_shared<ASTSettingsProfileElement>();
    ast->id_mode = true;

    if (parent_profile)
        ast->parent_profile = ::DB::toString(*parent_profile);

    ast->setting_name = setting_name;
    ast->value = value;
    ast->min_value = min_value;
    ast->max_value = max_value;
    ast->writability = writability;

    return ast;
}


std::shared_ptr<ASTSettingsProfileElement> SettingsProfileElement::toASTWithNames(const AccessControl & access_control) const
{
    auto ast = std::make_shared<ASTSettingsProfileElement>();

    if (parent_profile)
    {
        auto parent_profile_name = access_control.tryReadName(*parent_profile);
        if (parent_profile_name)
            ast->parent_profile = *parent_profile_name;
    }

    ast->setting_name = setting_name;
    ast->value = value;
    ast->min_value = min_value;
    ast->max_value = max_value;
    ast->writability = writability;

    return ast;
}


SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element);
}

SettingsProfileElements::SettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control)
{
    for (const auto & ast_element : ast.elements)
        emplace_back(*ast_element, access_control);
}


std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toAST() const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
    {
        auto element_ast = element.toAST();
        if (!element_ast->empty())
            res->elements.push_back(element_ast);
    }
    return res;
}

std::shared_ptr<ASTSettingsProfileElements> SettingsProfileElements::toASTWithNames(const AccessControl & access_control) const
{
    auto res = std::make_shared<ASTSettingsProfileElements>();
    for (const auto & element : *this)
    {
        auto element_ast = element.toASTWithNames(access_control);
        if (!element_ast->empty())
            res->elements.push_back(element_ast);
    }
    return res;
}


std::vector<UUID> SettingsProfileElements::findDependencies() const
{
    std::vector<UUID> res;
    for (const auto & element : *this)
    {
        if (element.parent_profile)
            res.push_back(*element.parent_profile);
    }
    return res;
}


void SettingsProfileElements::replaceDependencies(const std::unordered_map<UUID, UUID> & old_to_new_ids)
{
    for (auto & element : *this)
    {
        if (element.parent_profile)
        {
            auto id = *element.parent_profile;
            auto it_new_id = old_to_new_ids.find(id);
            if (it_new_id != old_to_new_ids.end())
            {
                auto new_id = it_new_id->second;
                element.parent_profile = new_id;
            }
        }
    }
}


void SettingsProfileElements::merge(const SettingsProfileElements & other)
{
    insert(end(), other.begin(), other.end());
}


Settings SettingsProfileElements::toSettings() const
{
    Settings res;
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty() && !isAllowBackupSetting(elem.setting_name) && elem.value)
            res.set(elem.setting_name, *elem.value);
    }
    return res;
}

SettingsChanges SettingsProfileElements::toSettingsChanges() const
{
    SettingsChanges res;
    for (const auto & elem : *this)
    {
        if (!elem.setting_name.empty() && !isAllowBackupSetting(elem.setting_name))
        {
            if (elem.value)
                res.push_back({elem.setting_name, *elem.value});
        }
    }
    return res;
}

SettingsConstraints SettingsProfileElements::toSettingsConstraints(const AccessControl & access_control) const
{
    SettingsConstraints res{access_control};
    for (const auto & elem : *this)
        if (!elem.setting_name.empty() && elem.isConstraint() && !isAllowBackupSetting(elem.setting_name))
            res.set(
                elem.setting_name,
                elem.min_value ? *elem.min_value : Field{},
                elem.max_value ? *elem.max_value : Field{},
                elem.writability ? *elem.writability : SettingConstraintWritability::WRITABLE);
    return res;
}

std::vector<UUID> SettingsProfileElements::toProfileIDs() const
{
    std::vector<UUID> res;
    for (const auto & elem : *this)
    {
        if (elem.parent_profile)
            res.push_back(*elem.parent_profile);
    }

    /// If some profile occurs multiple times (with some other settings in between),
    /// the latest occurrence overrides all the previous ones.
    removeDuplicatesKeepLast(res);

    return res;
}

bool SettingsProfileElements::isBackupAllowed() const
{
    for (const auto & setting : *this)
    {
        if (isAllowBackupSetting(setting.setting_name) && setting.value)
            return static_cast<bool>(SettingFieldBool{*setting.value});
    }
    return true;
}

bool SettingsProfileElements::isAllowBackupSetting(const String & setting_name)
{
    static constexpr std::string_view ALLOW_BACKUP_SETTING_NAME = "allow_backup";
    return Settings::Traits::resolveName(setting_name) == ALLOW_BACKUP_SETTING_NAME;
}


AlterSettingsProfileElements::AlterSettingsProfileElements(const SettingsProfileElements & ast)
{
    drop_all_settings = true;
    drop_all_profiles = true;
    add_settings = ast;
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTSettingsProfileElements & ast)
    : AlterSettingsProfileElements(SettingsProfileElements{ast})
{
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTSettingsProfileElements & ast, const AccessControl & access_control)
    : AlterSettingsProfileElements(SettingsProfileElements{ast, access_control})
{
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast)
{
    drop_all_settings = ast.drop_all_settings;
    drop_all_profiles = ast.drop_all_profiles;

    if (ast.add_settings)
        add_settings = SettingsProfileElements{*ast.add_settings};

    if (ast.modify_settings)
        modify_settings = SettingsProfileElements{*ast.modify_settings};

    if (ast.drop_settings)
        drop_settings = SettingsProfileElements{*ast.drop_settings};
}

AlterSettingsProfileElements::AlterSettingsProfileElements(const ASTAlterSettingsProfileElements & ast, const AccessControl & access_control)
{
    drop_all_settings = ast.drop_all_settings;
    drop_all_profiles = ast.drop_all_profiles;

    if (ast.add_settings)
        add_settings = SettingsProfileElements{*ast.add_settings, access_control};

    if (ast.modify_settings)
        modify_settings = SettingsProfileElements{*ast.modify_settings, access_control};

    if (ast.drop_settings)
        drop_settings = SettingsProfileElements{*ast.drop_settings, access_control};
}

void SettingsProfileElements::applyChanges(const AlterSettingsProfileElements & changes)
{
    /// 1. First drop profiles and settings which should be dropped.
    if (changes.drop_all_profiles)
    {
        for (auto & element : *this)
            element.parent_profile.reset(); /// This makes the element empty if it's a "profile" element.
    }

    if (changes.drop_all_settings)
    {
        for (auto & element : *this)
        {
            /// This makes the element empty if it's a "setting" element.
            /// Empty elements will be removed later at the end of this function.
            element.setting_name.clear();
            element.value.reset();
            element.min_value.reset();
            element.max_value.reset();
            element.writability.reset();
        }
    }

    auto apply_drop_profile = [&](const UUID & profile_id)
    {
        for (auto & element : *this)
        {
            if (element.parent_profile == profile_id)
                element.parent_profile.reset(); /// This makes the element empty if it corresponds to that profile.
        }
    };

    auto apply_drop_setting = [&](const String & setting_name, size_t end_index = -1)
    {
        if (end_index == static_cast<size_t>(-1))
            end_index = this->size();
        for (size_t i = 0; i != end_index; ++i)
        {
            auto & element = (*this)[i];
            if (element.setting_name == setting_name)
            {
                /// This makes the element empty if it corresponds to that setting.
                /// Empty elements will be removed later at the end of this function.
                element.setting_name.clear();
                element.value.reset();
                element.min_value.reset();
                element.max_value.reset();
                element.writability.reset();
            }
        }
    };

    for (const auto & drop : changes.drop_settings)
    {
        if (drop.parent_profile)
            apply_drop_profile(*drop.parent_profile);
        if (!drop.setting_name.empty())
            apply_drop_setting(drop.setting_name);
    }

    /// 2. Then add profiles which should be added.
    auto apply_add_profile = [&](const UUID & profile_id, bool check_if_exists = true)
    {
        if (check_if_exists)
            apply_drop_profile(profile_id);
        SettingsProfileElement new_element;
        new_element.parent_profile = profile_id;
        push_back(new_element);
    };

    for (const auto & add : changes.add_settings)
    {
        if (add.parent_profile)
            apply_add_profile(*add.parent_profile);
    }

    /// 3. Then add settings which should be added.
    auto apply_add_setting = [&](const SettingsProfileElement & add, bool check_if_exists = true)
    {
        if (check_if_exists)
            apply_drop_setting(add.setting_name);
        SettingsProfileElement new_element;
        new_element.setting_name = add.setting_name;
        new_element.value = add.value;
        new_element.min_value = add.min_value;
        new_element.max_value = add.max_value;
        new_element.writability = add.writability;
        push_back(new_element);
    };

    for (const auto & add : changes.add_settings)
    {
        if (!add.setting_name.empty())
            apply_add_setting(add);
    }

    /// 4. After that modify settings which should be modified.
    auto apply_modify_setting = [&](const SettingsProfileElement & modify, bool check_if_exists = true)
    {
        if (check_if_exists && !this->empty())
        {
            for (int i = static_cast<int>(this->size()) - 1; i >= 0; --i)
            {
                auto & element = (*this)[i];
                if (element.setting_name == modify.setting_name)
                {
                    if (modify.value)
                        element.value = modify.value;
                    if (modify.min_value)
                        element.min_value = modify.min_value;
                    if (modify.max_value)
                        element.max_value = modify.max_value;
                    if (modify.writability)
                        element.writability = modify.writability;
                    apply_drop_setting(modify.setting_name, i);
                    return;
                }
            }
        }
        apply_add_setting(modify, /* check_if_exists= */ false);
    };

    for (const auto & modify : changes.modify_settings)
    {
        chassert(!modify.parent_profile); /// There is no such thing as "MODIFY PROFILE".
        if (!modify.setting_name.empty())
            apply_modify_setting(modify);
    }

    /// 5. Finally remove empty elements from the result list of elements.
    std::erase_if(*this, [](const SettingsProfileElement & element) { return element.empty(); });
}

}
