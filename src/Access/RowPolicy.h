#pragma once

#include <Access/IAccessEntity.h>
#include <Access/RolesOrUsersSet.h>
#include <Access/Common/RowPolicyTypes.h>
#include <Core/Types.h>
#include <array>


namespace DB
{

/** Represents a row level security policy for a table.
  */
struct RowPolicy : public IAccessEntity
{
    using ConditionType = RowPolicyConditionType;
    using ConditionTypeInfo = RowPolicyConditionTypeInfo;
    using Conditions = std::array<String, static_cast<size_t>(ConditionType::MAX)>;
    static constexpr auto SELECT_FILTER = ConditionType::SELECT_FILTER;

    void setShortName(const String & short_name);
    void setDatabase(const String & database);
    void setTableName(const String & table_name);
    void setName(const String & short_name, const String & database, const String & table_name);
    void setName(const RowPolicyName & name);

    const String & getDatabase() const { return row_policy_name.database; }
    const String & getTableName() const { return row_policy_name.table_name; }
    const String & getShortName() const { return row_policy_name.short_name; }
    const RowPolicyName & getName() const { return row_policy_name; }

    /// Filter is a SQL conditional expression used to figure out which rows should be visible
    /// for user or available for modification. If the expression returns NULL or false for some rows
    /// those rows are silently suppressed.
    /// Check is a SQL condition expression used to check whether a row can be written into
    /// the table. If the expression returns NULL or false an exception is thrown.
    /// If a conditional expression here is empty it means no filtering is applied.
    void setCondition(ConditionType condition_type, const String & condition) { conditions[static_cast<size_t>(condition_type)] = condition; }
    const String & getCondition(ConditionType condition_type) const { return conditions[static_cast<size_t>(condition_type)]; }
    const Conditions & getConditions() const { return conditions; }

    /// Sets that the policy is permissive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setPermissive(bool permissive_ = true) { setRestrictive(!permissive_); }
    bool isPermissive() const { return !isRestrictive(); }

    /// Sets that the policy is restrictive.
    /// A row is only accessible if at least one of the permissive policies passes,
    /// in addition to all the restrictive policies.
    void setRestrictive(bool restrictive_ = true) { restrictive = restrictive_; }
    bool isRestrictive() const { return restrictive; }

    bool equal(const IAccessEntity & other) const override;
    std::shared_ptr<IAccessEntity> clone() const override { return cloneImpl<RowPolicy>(); }
    static constexpr const Type TYPE = Type::ROW_POLICY;
    Type getType() const override { return TYPE; }

    /// Which roles or users should use this row policy.
    RolesOrUsersSet to_roles;

private:
    void setName(const String &) override; /// Must not be called!

    RowPolicyName row_policy_name;
    Conditions conditions;
    bool restrictive = false;
};

using RowPolicyPtr = std::shared_ptr<const RowPolicy>;

}
