#pragma once

#include <Access/IAttributes.h>
#include <Access/SettingsConstraints.h>
#include <Common/SettingsChanges.h>


namespace DB
{
struct SettingsProfile : public IAttributes
{
    SettingsChanges settings;
    SettingsConstraints settings_constraints;

    static const Type TYPE;
    const Type & getType() const override { return TYPE; }
    std::shared_ptr<IAttributes> clone() const override { return cloneImpl<SettingsProfile>(); }
    bool equal(const IAttributes & other) const override;
    bool hasReferences(const UUID & id) const override;
    void removeReferences(const UUID & id) override;
};
}
