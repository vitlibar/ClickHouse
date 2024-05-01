#include <Parsers/ASTTargetTables.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/CommonParsers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

std::string_view toString(TargetTableKind kind)
{
    switch (kind)
    {
        case TargetTableKind::kTarget:       return "target";
        case TargetTableKind::kIntermediate: return "intermediate";
        case TargetTableKind::kData:         return "data";
        case TargetTableKind::kMetrics:      return "metrics";
        case TargetTableKind::kMetadata:     return "metadata";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

TargetTableKind parseTargetTableKindFromString(std::string_view str)
{
    for (auto kind : magic_enum::enum_values<TargetTableKind>())
    {
        if (toString(kind) == str)
            return kind;
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: Unexpected string {}", __FUNCTION__, str);
}

void ASTTargetTables::setTableId(TargetTableKind kind, const StorageID & table_id_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.table_id = table_id_;
            return;
        }
    }
    if (table_id_)
        targets.emplace_back(kind).table_id = table_id_;
}

void ASTTargetTables::setCurrentDatabase(const String & current_database)
{
    for (auto & target : targets)
    {
        auto & table_id = target.table_id;
        if (!table_id.table_name.empty() && table_id.database_name.empty())
            table_id.database_name = current_database;
    }
}

void ASTTargetTables::setInnerUUID(TargetTableKind kind, const UUID & inner_uuid_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            target.inner_uuid = inner_uuid_;
            return;
        }
    }
    if (inner_uuid_ != UUIDHelpers::Nil)
        targets.emplace_back(kind).inner_uuid = inner_uuid_;
}

void ASTTargetTables::resetInnerUUIDs()
{
    for (auto & target : targets)
        target.inner_uuid = UUIDHelpers::Nil;
}

void ASTTargetTables::setStorage(TargetTableKind kind, ASTPtr storage_)
{
    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (storage_)
                setOrReplace(target.storage, storage_);
            else
                reset(target.storage);
            return;
        }
    }
    if (storage_)
        set(targets.emplace_back(kind).storage, storage_);
}

const ASTTargetTables::Target & ASTTargetTables::Target::getEmpty(TargetTableKind kind_)
{
    switch (kind_)
    {
        /// `s_empty` is a different variable for each TargetTableKind.
        case TargetTableKind::kTarget:       { static const Target s_empty{kind_}; return s_empty; }
        case TargetTableKind::kIntermediate: { static const Target s_empty{kind_}; return s_empty; }
        case TargetTableKind::kData:         { static const Target s_empty{kind_}; return s_empty; }
        case TargetTableKind::kMetrics:      { static const Target s_empty{kind_}; return s_empty; }
        case TargetTableKind::kMetadata:     { static const Target s_empty{kind_}; return s_empty; }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind_);
}

const ASTTargetTables::Target & ASTTargetTables::getTarget(TargetTableKind kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            return target;
    }
    return Target::getEmpty(kind);
}

ASTPtr ASTTargetTables::clone() const
{
    auto res = std::make_shared<ASTTargetTables>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.storage)
            res->set(target.storage, target.storage->clone());
    }
    return res;
}

void ASTTargetTables::formatHelper(const FormatSettings & s, FormatState & state, FormatStateStacked frame,
                                   std::optional<TargetTableKind> show_kinds_min, std::optional<TargetTableKind> show_kinds_max,
                                   bool show_table_id, bool show_storage, bool show_inner_uuid) const
{
    for (const auto & target : targets)
    {
        auto kind = target.kind;
        if (show_kinds_min && (kind < *show_kinds_min))
            continue;
        if (show_kinds_max && (kind > *show_kinds_max))
            continue;
        if (target.table_id)
        {
            if (show_table_id)
            {
                auto keyword = kindToKeywordForTableId(kind);
                chassert(keyword);
                if (keyword)
                {
                    s.ostr <<  " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
                           << (s.hilite ? hilite_none : "") << " "
                           << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
                           << backQuoteIfNeed(target.table_id.table_name);
                }
            }
        }
        if ((target.inner_uuid != UUIDHelpers::Nil) && show_inner_uuid)
        {
            auto keyword = kindToKeywordForInnerUUID(kind);
            chassert(keyword);
            if (keyword)
            {
                s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*keyword)
                       << (s.hilite ? hilite_none : "") << " " << quoteString(toString(target.inner_uuid));
            }
        }
        if (target.storage && show_storage)
        {
            auto prefix = kindToPrefixForStorage(kind);
            if (prefix) /// If there is no prefix then we just output the storage without a prefix.
                s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(*prefix) << (s.hilite ? hilite_none : "");
            target.storage->formatImpl(s, state, frame);
        }
    }
}

std::optional<Keyword> ASTTargetTables::kindToKeywordForTableId(TargetTableKind kind)
{
    switch (kind)
    {
        case TargetTableKind::kTarget:       return Keyword::TO;
        case TargetTableKind::kIntermediate: return std::nullopt; /// WindowView: the table with intermediate results is always internal,
                                                                  /// it can't be specified as external
        case TargetTableKind::kData:         return Keyword::DATA;
        case TargetTableKind::kMetrics:      return Keyword::METRICS;
        case TargetTableKind::kMetadata:     return Keyword::METADATA;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

std::optional<Keyword> ASTTargetTables::kindToPrefixForStorage(TargetTableKind kind)
{
    switch (kind)
    {
        case TargetTableKind::kTarget:       return std::nullopt;      /// ENGINE = ...
                                                                       /// (MaterializedView: engine for an internal table is specified
                                                                       /// without any prefix, i.e. just "ENGINE = ...")
        case TargetTableKind::kIntermediate: return Keyword::INNER;    /// INNER ENGINE = ...
                                                                       /// (WindowView: the engine of the table with intermediate results
                                                                       /// is specified with prefix "INNER")
        case TargetTableKind::kData:         return Keyword::DATA;     /// DATA ENGINE = ...
        case TargetTableKind::kMetrics:      return Keyword::METRICS;  /// METRICS ENGINE = ...
        case TargetTableKind::kMetadata:     return Keyword::METADATA; /// METADATA ENGINE = ...
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

std::optional<Keyword> ASTTargetTables::kindToKeywordForInnerUUID(TargetTableKind kind)
{
    switch (kind)
    {
        case TargetTableKind::kTarget:       return Keyword::TO_INNER_UUID;       /// TO INNER UUID 'XXX'
        case TargetTableKind::kIntermediate: return std::nullopt;                 /// WindowView: the uuid of the table with intermediate results
                                                                                  /// is never specified in the query (because
                                                                                  /// otherwise "INNER INNER UUID 'XXX'" would look wierd)
        case TargetTableKind::kData:         return Keyword::DATA_INNER_UUID;     /// DATA INNER UUID 'XXX'
        case TargetTableKind::kMetrics:      return Keyword::METRICS_INNER_UUID;  /// METRICS INNER UUID 'XXX'
        case TargetTableKind::kMetadata:     return Keyword::METADATA_INNER_UUID; /// METADATA INNER UUID 'XXX'
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

}
