#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST_fwd.h>


namespace DB
{

/// Returns the scale used in a specified timestamp type if it's a decimal, or 0 otherwise.
UInt32 getTimeSeriesTimestampScale(const DataTypePtr & timestamp_type);

/// Extracts a timestamp from a Field.
DecimalField<DateTime64> getTimeSeriesTimestamp(const Field & field, UInt32 default_scale);
DecimalField<DateTime64> getTimeSeriesTimestamp(const Field & field, const DataTypePtr & type, UInt32 default_scale);

/// Extracts an interval from a Field.
DecimalField<Decimal64> getTimeSeriesInterval(const Field & field, UInt32 default_scale);
DecimalField<Decimal64> getTimeSeriesInterval(const Field & field, const DataTypePtr & type, UInt32 default_scale);

/// Converts a timestamp to SQL.
ASTPtr timeSeriesTimestampToAST(const DecimalField<DateTime64> & timestamp, const DataTypePtr & timestamp_type);

/// Converts a time interval to SQL.
ASTPtr timeSeriesIntervalToAST(const DecimalField<Decimal64> & interval);

/// Adds an offset to a timestamp.
DecimalField<DateTime64> addTimeSeriesInterval(const DecimalField<DateTime64> & timestamp, const DecimalField<Decimal64> & interval);

/// Subtracts an offset from a timestamp.
DecimalField<DateTime64> subtractTimeSeriesInterval(const DecimalField<DateTime64> & timestamp, const DecimalField<Decimal64> & interval);

/// Calculates the interval between [min_time] and [max_time].
/// The function basically returns (max_time - min_time).
DecimalField<Decimal64> getTimeSeriesInterval(const DecimalField<DateTime64> & min_time, const DecimalField<DateTime64> & max_time);

/// Returns the number of steps between `start_time` and `end_time`, including `start_time` and `end_time`.
size_t getNumberOfTimeSeriesSteps(const DecimalField<DateTime64> & start_time, const DecimalField<DateTime64> & end_time, const DecimalField<Decimal64> & step);

/// Increases a timestamp to make it divisible by `step`.
DecimalField<DateTime64> roundUpTimeSeriesTimestamp(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step);

/// Decreases a timestamp to make it divisible by `step`.
DecimalField<DateTime64> roundDownTimeSeriesTimestamp(const DecimalField<DateTime64> & time, const DecimalField<Decimal64> & step);

}
