#pragma once

#include <Parsers/IAST.h>
#include <Interpreters/StorageID.h>


namespace DB
{
class ASTStorage;
enum class Keyword : size_t;

/// Information about the target table for a materialized view, or a window view, or a TimeSeries table.
struct ViewTarget
{
    enum class Kind
    {
        /// Target table for a materialized view or a window view.
        Target,

        /// Table with intermediate results for a window view.
        Intermediate,

        /// The "data" table for a TimeSeries table, contains time series.
        Data,

        /// The "tags" table for a TimeSeries table, contains identifiers for each combination of a metric name and tags (labels).
        Tags,

        /// The "metrics" table for a TimeSeries table, contains general information (metadata) about metrics.
        Metrics,
    };

    Kind kind;

    /// StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    StorageID table_id = StorageID::createEmpty();

    /// UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    UUID inner_uuid = UUIDHelpers::Nil;

    /// Table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    ASTStorage * inner_storage = nullptr;

    explicit ViewTarget(Kind kind_) : kind(kind_) {}
    static const ViewTarget & getEmpty(Kind kind_);
};

/// Converts ViewTarget::Kind to a string.
std::string_view toString(ViewTarget::Kind kind);
void parseFromString(ViewTarget::Kind & out, std::string_view str);


/// Information about all the target tables for a view.
class ASTViewTargets : public IAST
{
public:
    using Kind = ViewTarget::Kind;
    std::vector<ViewTarget> targets;

    /// Returns information about a target table.
    /// The function returns an empty ViewTarget if there is no information specified.
    const ViewTarget & getTarget(Kind kind = Kind::Target) const;

    /// Sets the StorageID of the target table, if it's not inner.
    /// That storage ID can be seen for example after "TO" in a statement like CREATE MATERIALIZED VIEW ... TO ...
    void setTableId(Kind kind, const StorageID & table_id_);
    void setTableId(const StorageID & table_id_) { setTableId(Kind::Target, table_id_); }
    const StorageID & getTableId(Kind kind = Kind::Target) const { return getTarget(kind).table_id; }
    bool hasTableId(Kind kind = Kind::Target) const { return !getTableId(kind).empty(); }

    /// Replaces an empty database in the StorageID of the target table with a specified database.
    void setCurrentDatabase(const String & current_database);

    /// Sets the UUID of the target table, if it's inner.
    /// The UUID is calculated automatically and can be seen for example after "TO INNER UUID" in a statement like
    /// CREATE MATERIALIZED VIEW ... TO INNER UUID ...
    void setInnerUUID(Kind kind, const UUID & inner_uuid_);
    void setInnerUUID(const UUID & inner_uuid_) { setInnerUUID(Kind::Target, inner_uuid_); }
    const UUID & getInnerUUID(Kind kind = Kind::Target) const { return getTarget(kind).inner_uuid; }
    void resetInnerUUIDs();

    /// Sets the table engine of the target table, if it's inner.
    /// That engine can be seen for example after "ENGINE" in a statement like CREATE MATERIALIZED VIEW ... ENGINE ...
    void setInnerStorage(Kind kind, ASTPtr inner_storage_);
    void setInnerStorage(ASTPtr inner_storage_) { setInnerStorage(Kind::Target, inner_storage_); }
    const ASTStorage * getInnerStorage(Kind kind = Kind::Target) const { return getTarget(kind).inner_storage; }
    ASTStorage * getInnerStorage(Kind kind = Kind::Target);

    String getID(char) const override { return "ViewTargets"; }

    ASTPtr clone() const override;

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;

    /// Formats information only about a specific target table.
    void formatTarget(Kind kind, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const;
    static void formatTarget(const ViewTarget & target, const FormatSettings & s, FormatState & state, FormatStateStacked frame);

    /// Helper functions for class ParserViewTargets. Assumes the kind is Data or Tags or Metrics.
    static Keyword kindToKeywordForTableId(Kind kind);
    static Keyword kindToKeywordForInnerUUID(Kind kind);
    static Keyword kindToPrefixForInnerStorage(Kind kind);

protected:
    void forEachPointerToChild(std::function<void(void**)> f) override
    {
        for (auto & target : targets)
            f(reinterpret_cast<void **>(&target.inner_storage));
    }
};

}
