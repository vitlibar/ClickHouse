#include <Access/RowPolicyCache.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/AccessControlManager.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <base/range.h>
#include <boost/smart_ptr/make_shared.hpp>
#include <Core/Defines.h>


namespace DB
{
namespace
{
    using FilterType = RowPolicy::FilterType;

    /// Accumulates conditions from multiple row policies and joins them using the AND logical operation.
    class FilterMixer
    {
    public:
        void add(const ASTPtr & filter, bool is_restrictive)
        {
            if (is_restrictive)
                restrictions.push_back(filter);
            else
                permissions.push_back(filter);
        }

        ASTPtr getResult() &&
        {
            /// Process permissive conditions.
            restrictions.push_back(makeASTForLogicalOr(std::move(permissions)));

            /// Process restrictive conditions.
            auto filter = makeASTForLogicalAnd(std::move(restrictions));

            bool value;
            if (tryGetLiteralBool(filter.get(), value) && value)
                filter = nullptr;  /// The condition is always true, no need to check it.

            return filter;
        }

    private:
        ASTs permissions;
        ASTs restrictions;
    };
}


void RowPolicyCache::PolicyInfo::setPolicy(const RowPolicyPtr & policy_)
{
    policy = policy_;
    roles = &policy->to_roles;
    database_and_table_name = std::make_shared<std::pair<String, String>>(policy->getDatabase(), policy->getTableName());

    for (auto type : collections::range(0, FilterType::MAX))
    {
        const String & filter = policy->getFilter(type);
        auto type_n = static_cast<size_t>(type);
        parsed_filters[type_n] = nullptr;
        if (filter.empty())
            continue;

        bool already_parsed = false;
        for (auto parsed_type : collections::range(0, type))
        {
            if (filter == policy->getFilter(parsed_type))
            {
                /// The condition is already parsed before.
                parsed_filters[type_n] = parsed_filters[static_cast<size_t>(parsed_type)];
                already_parsed = true;
                break;
            }
        }

        if (already_parsed)
            continue;

        /// Try to parse the condition.
        try
        {
            ParserExpression parser;
            parsed_filters[type_n] = parseQuery(parser, filter, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        }
        catch (...)
        {
            tryLogCurrentException(
                &Poco::Logger::get("RowPolicy"),
                String("Could not parse the condition ") + toString(type) + " of row policy "
                    + backQuote(policy->getName().toString()));
        }
    }
}


RowPolicyCache::RowPolicyCache(const AccessControlManager & access_control_manager_)
    : access_control_manager(access_control_manager_)
{
}

RowPolicyCache::~RowPolicyCache() = default;


std::shared_ptr<const EnabledRowPolicies> RowPolicyCache::getEnabledRowPolicies(const UUID & user_id, const boost::container::flat_set<UUID> & enabled_roles)
{
    std::lock_guard lock{mutex};
    ensureAllRowPoliciesRead();

    EnabledRowPolicies::Params params;
    params.user_id = user_id;
    params.enabled_roles = enabled_roles;
    auto it = enabled_row_policies.find(params);
    if (it != enabled_row_policies.end())
    {
        auto from_cache = it->second.lock();
        if (from_cache)
            return from_cache;
        enabled_row_policies.erase(it);
    }

    auto res = std::shared_ptr<EnabledRowPolicies>(new EnabledRowPolicies(params));
    enabled_row_policies.emplace(std::move(params), res);
    mixFiltersFor(*res);
    return res;
}


void RowPolicyCache::ensureAllRowPoliciesRead()
{
    /// `mutex` is already locked.
    if (all_policies_read)
        return;
    all_policies_read = true;

    subscription = access_control_manager.subscribeForChanges<RowPolicy>(
        [&](const UUID & id, const AccessEntityPtr & entity)
        {
            if (entity)
                rowPolicyAddedOrChanged(id, typeid_cast<RowPolicyPtr>(entity));
            else
                rowPolicyRemoved(id);
        });

    for (const UUID & id : access_control_manager.findAll<RowPolicy>())
    {
        auto quota = access_control_manager.tryRead<RowPolicy>(id);
        if (quota)
            all_policies.emplace(id, PolicyInfo(quota));
    }
}


void RowPolicyCache::rowPolicyAddedOrChanged(const UUID & policy_id, const RowPolicyPtr & new_policy)
{
    std::lock_guard lock{mutex};
    auto it = all_policies.find(policy_id);
    if (it == all_policies.end())
    {
        it = all_policies.emplace(policy_id, PolicyInfo(new_policy)).first;
    }
    else
    {
        if (it->second.policy == new_policy)
            return;
    }

    auto & info = it->second;
    info.setPolicy(new_policy);
    mixFilters();
}


void RowPolicyCache::rowPolicyRemoved(const UUID & policy_id)
{
    std::lock_guard lock{mutex};
    all_policies.erase(policy_id);
    mixFilters();
}


void RowPolicyCache::mixFilters()
{
    /// `mutex` is already locked.
    for (auto i = enabled_row_policies.begin(), e = enabled_row_policies.end(); i != e;)
    {
        auto elem = i->second.lock();
        if (!elem)
            i = enabled_row_policies.erase(i);
        else
        {
            mixFiltersFor(*elem);
            ++i;
        }
    }
}


void RowPolicyCache::mixFiltersFor(EnabledRowPolicies & enabled)
{
    /// `mutex` is already locked.

    using MapOfMixedFilters = EnabledRowPolicies::MapOfMixedFilters;
    using MixedFilterKey = EnabledRowPolicies::MixedFilterKey;
    using Hash = EnabledRowPolicies::Hash;

    struct MixerWithNames
    {
        FilterMixer mixer;
        std::shared_ptr<const std::pair<String, String>> database_and_table_name;
    };

    std::unordered_map<MixedFilterKey, MixerWithNames, Hash> map_of_mixers;

    for (const auto & [policy_id, info] : all_policies)
    {
        const auto & policy = *info.policy;
        bool match = info.roles->match(enabled.params.user_id, enabled.params.enabled_roles);
        MixedFilterKey key;
        key.database = info.database_and_table_name->first;
        key.table_name = info.database_and_table_name->second;
        for (auto type : collections::range(0, FilterType::MAX))
        {
            auto type_n = static_cast<size_t>(type);
            if (info.parsed_filters[type_n])
            {
                key.filter_type = type;
                auto & mixer = map_of_mixers[key];
                mixer.database_and_table_name = info.database_and_table_name;
                if (match)
                    mixer.mixer.add(info.parsed_filters[type_n], policy.isRestrictive());
            }
        }
    }

    auto map_of_mixed_filters = boost::make_shared<MapOfMixedFilters>();
    for (auto & [key, mixer] : map_of_mixers)
    {
        auto & mixed_filter = (*map_of_mixed_filters)[key];
        mixed_filter.database_and_table_name = mixer.database_and_table_name;
        mixed_filter.ast = std::move(mixer.mixer).getResult();
    }

    enabled.map_of_mixed_filters.store(map_of_mixed_filters);
}

}
