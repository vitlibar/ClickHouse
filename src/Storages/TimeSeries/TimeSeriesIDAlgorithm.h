#pragma once

#include <Core/SettingsEnums.h>


namespace DB
{

/// Algorithm used to calculate id from the metric name and tag names and values.
enum class TimeSeriesIDAlgorithm
{
    /// A single SipHash is calculated for both the metric name and tag names and tag values.
    /// The "id" column must be either "UInt64" or "UInt128".
    SipHash,

    /// Two 64bit SipHashes are calculated - the first one for the metric name and the second one for tag names and tag values.
    /// Then the first SipHash becomes the lowest 64 bits and the second SipHash becomes the highest 64 bits of a 128-bit result.
    /// The "id" column must be "UInt128".
    SipHash_MetricNameLow64_And_TagsHigh64,
};

DECLARE_SETTING_ENUM(TimeSeriesIDAlgorithm)

}
