#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/Decimal.h>


namespace DB
{
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

/// Converts a timestamp to SQL.
ASTPtr timeSeriesTimestampToAST(DateTime64 timestamp, const DataTypePtr & timestamp_data_type);

/// Converts a duration to SQL.
ASTPtr timeSeriesDurationToAST(Decimal64 duration, const DataTypePtr & timestamp_data_type);

}
