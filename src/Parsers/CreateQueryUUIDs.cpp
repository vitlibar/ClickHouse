#include <Parsers/CreateQueryUUIDs.h>

#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

namespace
{
    void setInnerUUID(CreateQueryUUIDs & res, TargetTableKind kind, const UUID & inner_uuid)
    {
        for (auto & pair : res.inner_uuids)
        {
            if (pair.first == kind)
            {
                pair.second = inner_uuid;
                return;
            }
        }
        if (inner_uuid != UUIDHelpers::Nil)
            res.inner_uuids.emplace_back(kind, inner_uuid);
    }
}

CreateQueryUUIDs::CreateQueryUUIDs(const ASTCreateQuery & query)
{
    uuid = query.uuid;
    if (query.target_tables)
    {
        for (const auto & target : query.target_tables->targets)
            setInnerUUID(*this, target.kind, target.inner_uuid);
    }
}

bool CreateQueryUUIDs::empty() const
{
    if (uuid != UUIDHelpers::Nil)
        return false;
    for (const auto & [_, inner_uuid] : inner_uuids)
    {
        if (inner_uuid != UUIDHelpers::Nil)
            return false;
    }
    return true;
}

String CreateQueryUUIDs::toString() const
{
    WriteBufferFromOwnString out;
    out << "{";
    bool need_comma = false;
    auto print_key_and_value = [&](std::string_view name_, const UUID & uuid_)
    {
        if (std::exchange(need_comma, true))
            out << ", ";
        out << "\"" << name_ << "\": \"" << uuid_ << "\"";
    };
    if (uuid != UUIDHelpers::Nil)
        print_key_and_value("uuid", uuid);
    for (const auto & [kind, inner_uuid] : inner_uuids)
    {
        if (inner_uuid != UUIDHelpers::Nil)
            print_key_and_value(::DB::toString(kind), inner_uuid);
    }
    out << "}";
    return out.str();
}

CreateQueryUUIDs CreateQueryUUIDs::fromString(const String & str)
{
    ReadBufferFromString in{str};
    CreateQueryUUIDs res;
    skipWhitespaceIfAny(in);
    in >> "{";
    skipWhitespaceIfAny(in);
    char c;
    while (in.peek(c) && c != '}')
    {
        String name;
        String uuid_value_str;
        readDoubleQuotedString(name, in);
        skipWhitespaceIfAny(in);
        in >> ":";
        skipWhitespaceIfAny(in);
        readDoubleQuotedString(uuid_value_str, in);
        skipWhitespaceIfAny(in);
        UUID uuid_value = parse<UUID>(uuid_value_str);
        if (name == "uuid")
        {
            res.uuid = uuid_value;
        }
        else
        {
            auto kind = parseTargetTableKindFromString(name);
            setInnerUUID(res, kind, uuid_value);
        }
        if (in.peek(c) && c == ',')
        {
            in.ignore(1);
            skipWhitespaceIfAny(in);
        }
    }
    in >> "}";
    return res;
}

void ASTCreateQuery::setUUIDs(const CreateQueryUUIDs & uuids)
{
    uuid = uuids.uuid;

    if (target_tables)
        target_tables->resetInnerUUIDs();

    if (!uuids.inner_uuids.empty())
    {
        if (!target_tables)
            set(target_tables, std::make_shared<ASTTargetTables>());

        for (const auto & [kind, inner_uuid] : uuids.inner_uuids)
        {
            if (inner_uuid != UUIDHelpers::Nil)
                target_tables->setInnerUUID(kind, inner_uuid);
        }
    }
}

CreateQueryUUIDs ASTCreateQuery::generateRandomUUIDs(bool always_generate_new_uuids)
{
    CreateQueryUUIDs res;

    if (!always_generate_new_uuids)
        res = CreateQueryUUIDs{*this};

    if (res.uuid == UUIDHelpers::Nil)
        res.uuid = UUIDHelpers::generateV4();

    auto generate_uuid = [&](TargetTableKind kind_)
    {
        const auto & target = getTarget(kind_);
        if ((!target.table_id && ((target.inner_uuid == UUIDHelpers::Nil) || always_generate_new_uuids)))
            setInnerUUID(res, kind_, UUIDHelpers::generateV4());
    };

    if (!attach)
    {
        /// If destination table (to_table_id) is not specified for materialized view,
        /// then MV will create inner table. We should generate UUID of inner table here.
        if (is_materialized_view)
            generate_uuid(TargetTableKind::kTarget);

        if (is_time_series_table)
        {
            generate_uuid(TargetTableKind::kData);
            generate_uuid(TargetTableKind::kMetrics);
            generate_uuid(TargetTableKind::kMetadata);
        }
    }

    setUUIDs(res);
    return res;
}

}
