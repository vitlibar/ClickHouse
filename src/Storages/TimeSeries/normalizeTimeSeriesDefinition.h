#pragma once

#include <DataTypes/IDataType.h>
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{
class ASTCreateQuery;
struct TimeSeriesSettings;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Computes and stores INNER COLUMNS for each inner target table.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
/// Returns true if the query was modified.
bool normalizeTimeSeriesDefinition(
    ASTCreateQuery & create_query, const ContextPtr & context, LoadingStrictnessLevel mode, bool is_restore_from_backup);

/// Builds the default id-generator expression for a given `id_type`
/// (e.g. `reinterpretAsUUID(sipHash128(...))` for `UUID`).
ASTPtr makeASTForTimeSeriesIDGenerator(
    const DataTypePtr & id_type, const TimeSeriesSettings & settings, const StorageID & for_error);

}
