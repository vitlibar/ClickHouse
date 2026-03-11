#pragma once

#include <Interpreters/Context_fwd.h>


namespace DB
{
class ASTCreateQuery;

/// Normalizes a TimeSeries table definition.
/// Adds missing columns to the definition and reorders all the columns in the canonical way.
/// Also adds engines of inner tables to the definition if they aren't specified yet.
void normalizeTimeSeriesDefinition(ASTCreateQuery & create_query, const ContextPtr & local_context);

}
