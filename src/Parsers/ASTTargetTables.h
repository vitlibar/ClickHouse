#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

enum class TargetTableKind
{
    /// Destination table for a materialized view, or for a window view.
    kTarget,

    /// Table with intermediate results for a window view.
    kIntermediate,

    /// Data tables for a TimeSeries table.
    kData,
    kTags,
    kMetrics,
};

std::string_view toString(TargetTableKind kind);
TargetTableKind parseTargetTableKindFromString(std::string_view str);


/// Target tables for a table - for example the destination table for a materialized view.
class ASTTargetTables : public IAST
{
public:
    struct Target
    {
        TargetTableKind kind;
        StorageID table_id = StorageID::createEmpty();
        UUID inner_uuid = UUIDHelpers::Nil;
        ASTStorage * storage = nullptr;
        explicit Target(TargetTableKind kind_) : kind(kind_) {}
        static const Target & getEmpty(TargetTableKind kind_);
    };

    using Targets = std::vector<Target>;
    Targets targets;

    /// The function returns an empty Target if there is no Target of the specified kind.
    const Target & getTarget(TargetTableKind kind = TargetTableKind::kTarget) const;

    void setTableId(TargetTableKind kind, const StorageID & table_id_);
    void setTableId(const StorageID & table_id_) { setTableId(TargetTableKind::kTarget, table_id_); }
    const StorageID & getTableId(TargetTableKind kind = TargetTableKind::kTarget) { return getTarget(kind).table_id; }
    void setCurrentDatabase(const String & current_database);

    void setInnerUUID(TargetTableKind kind, const UUID & inner_uuid_);
    void setInnerUUID(const UUID & inner_uuid_) { setInnerUUID(TargetTableKind::kTarget, inner_uuid_); }
    const UUID & getInnerUUID(TargetTableKind kind = TargetTableKind::kTarget) { return getTarget(kind).inner_uuid; }
    void resetInnerUUIDs();

    void setStorage(TargetTableKind kind, ASTPtr storage_);
    void setStorage(ASTPtr storage_) { setStorage(TargetTableKind::kTarget, storage_); }
    ASTStorage * getStorage(TargetTableKind kind = TargetTableKind::kTarget) const { return getTarget(kind).storage; }

    String getID(char) const override { return "Target tables"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override { return formatHelper(s, state, std::move(frame)); }

    void formatHelper(const FormatSettings & s, FormatState & state, FormatStateStacked frame,
                      std::optional<TargetTableKind> show_kinds_min = {}, std::optional<TargetTableKind> show_kinds_max = {},
                      bool show_table_id = true, bool show_storage = true, bool show_inner_uuid = true) const;

    static std::optional<Keyword> kindToKeywordForTableId(TargetTableKind kind);
    static std::optional<Keyword> kindToPrefixForStorage(TargetTableKind kind);
    static std::optional<Keyword> kindToKeywordForInnerUUID(TargetTableKind kind);

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        for (auto & target : targets)
            f(reinterpret_cast<void **>(&target.storage));
    }
};

}
