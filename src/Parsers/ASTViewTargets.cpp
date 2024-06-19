#include <Parsers/ASTViewTargets.h>

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

using Kind = ViewTarget::Kind;


std::string_view toString(Kind kind)
{
    switch (kind)
    {
        case Kind::Target:  return "target";
        case Kind::Inner:   return "inner";
        case Kind::Data:    return "data";
        case Kind::Tags:    return "tags";
        case Kind::Metrics: return "metrics";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
}

void parseFromString(Kind & out, std::string_view str)
{
    for (auto kind : magic_enum::enum_values<Kind>())
    {
        if (toString(kind) == str)
        {
            out = kind;
            return;
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "{}: Unexpected string {}", __FUNCTION__, str);
}

const ViewTarget & ViewTarget::getEmpty(Kind kind_)
{
    switch (kind_)
    {
        /// `s_empty` is a different variable for each Kind.
        case Kind::Target:  { static const ViewTarget s_empty{kind_}; return s_empty; }
        case Kind::Inner:   { static const ViewTarget s_empty{kind_}; return s_empty; }
        case Kind::Data:    { static const ViewTarget s_empty{kind_}; return s_empty; }
        case Kind::Tags:    { static const ViewTarget s_empty{kind_}; return s_empty; }
        case Kind::Metrics: { static const ViewTarget s_empty{kind_}; return s_empty; }
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind_);
}


void ASTViewTargets::setTableId(Kind kind, const StorageID & table_id_)
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

void ASTViewTargets::setCurrentDatabase(const String & current_database)
{
    for (auto & target : targets)
    {
        auto & table_id = target.table_id;
        if (!table_id.table_name.empty() && table_id.database_name.empty())
            table_id.database_name = current_database;
    }
}

void ASTViewTargets::setInnerUUID(Kind kind, const UUID & inner_uuid_)
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

void ASTViewTargets::resetInnerUUIDs()
{
    for (auto & target : targets)
        target.inner_uuid = UUIDHelpers::Nil;
}

void ASTViewTargets::setTableEngine(Kind kind, ASTPtr table_engine_)
{
    auto new_table_engine = typeid_cast<std::shared_ptr<ASTStorage>>(table_engine_);
    if (!new_table_engine && table_engine_)
        throw Exception(DB::ErrorCodes::LOGICAL_ERROR, "Bad cast from type {} to ASTStorage", table_engine_->getID());

    for (auto & target : targets)
    {
        if (target.kind == kind)
        {
            if (target.table_engine == new_table_engine)
                return;
            if (new_table_engine)
                children.push_back(new_table_engine);
            if (target.table_engine)
                std::erase(children, target.table_engine);
            target.table_engine = new_table_engine;
            return;
        }
    }

    if (new_table_engine)
    {
        targets.emplace_back(kind).table_engine = new_table_engine;
        children.push_back(new_table_engine);
    }
}

const ViewTarget & ASTViewTargets::getTarget(Kind kind) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            return target;
    }
    return ViewTarget::getEmpty(kind);
}

ASTPtr ASTViewTargets::clone() const
{
    auto res = std::make_shared<ASTViewTargets>(*this);
    res->children.clear();
    for (auto & target : res->targets)
    {
        if (target.table_engine)
            res->children.push_back(target.table_engine);
    }
    return res;
}

void ASTViewTargets::formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
        formatTarget(target, s, state, frame);
}

void ASTViewTargets::formatTarget(Kind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const
{
    for (const auto & target : targets)
    {
        if (target.kind == kind)
            formatTarget(target, s, state, frame);
    }
}

void ASTViewTargets::formatTarget(const ViewTarget & target, const FormatSettings & s, FormatState & state, FormatStateStacked frame)
{
    if (target.table_id)
    {
        s.ostr <<  " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToKeywordForTableId(target.kind))
               << (s.hilite ? hilite_none : "") << " "
               << (!target.table_id.database_name.empty() ? backQuoteIfNeed(target.table_id.database_name) + "." : "")
               << backQuoteIfNeed(target.table_id.table_name);
    }

    if (target.inner_uuid != UUIDHelpers::Nil)
    {
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToKeywordForInnerUUID(target.kind))
               << (s.hilite ? hilite_none : "") << " " << quoteString(toString(target.inner_uuid));
    }

    if (target.table_engine)
    {
        s.ostr << " " << (s.hilite ? hilite_keyword : "") << toStringView(kindToPrefixForInnerStorage(target.kind)) << (s.hilite ? hilite_none : "");
        target.table_engine->formatImpl(s, state, frame);
    }
}

Keyword ASTViewTargets::kindToKeywordForTableId(Kind kind)
{
    switch (kind)
    {
        case Kind::Data:
            return Keyword::DATA; /// DATA mydb.mydata

        case Kind::Tags:
            return Keyword::TAGS; /// TAGS mydb.mytags

        case Kind::Metrics:
            return Keyword::METRICS; /// METRICS mydb.mymetrics

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

Keyword ASTViewTargets::kindToPrefixForInnerStorage(Kind kind)
{
    switch (kind)
    {
        case Kind::Data:
            return Keyword::DATA;     /// DATA ENGINE = MergeTree()

        case Kind::Tags:
            return Keyword::TAGS;     /// TAGS ENGINE = MergeTree()

        case Kind::Metrics:
            return Keyword::METRICS;  /// METRICS ENGINE = MergeTree()

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

Keyword ASTViewTargets::kindToKeywordForInnerUUID(Kind kind)
{
    switch (kind)
    {
        case Kind::Data:
            return Keyword::DATA_INNER_UUID;     /// DATA INNER UUID 'XXX'

        case Kind::Tags:
            return Keyword::TAGS_INNER_UUID;     /// TAGS INNER UUID 'XXX'

        case Kind::Metrics:
            return Keyword::METRICS_INNER_UUID;  /// METRICS INNER UUID 'XXX'

        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} doesn't support kind {}", __FUNCTION__, kind);
    }
}

void ASTViewTargets::forEachPointerToChild(std::function<void(void**)> f)
{
    for (auto & target : targets)
    {
        if (target.table_engine)
        {
            ASTStorage * new_table_engine = target.table_engine.get();
            f(reinterpret_cast<void **>(&new_table_engine));
            if (new_table_engine != target.table_engine.get())
            {
                if (new_table_engine)
                    target.table_engine = typeid_cast<std::shared_ptr<ASTStorage>>(new_table_engine->ptr());
                else
                    target.table_engine.reset();
            }
        }
    }
}

}
