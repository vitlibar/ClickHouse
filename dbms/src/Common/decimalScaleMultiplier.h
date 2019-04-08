#pragma once

#include <Core/Types.h>
#include <Common/intExp.h>

namespace DB
{

template <typename T> inline T decimalScaleMultiplier(UInt32 scale);
template <> inline Int32 decimalScaleMultiplier<Int32>(UInt32 scale) { return common::exp10_i32(scale); }
template <> inline Int64 decimalScaleMultiplier<Int64>(UInt32 scale) { return common::exp10_i64(scale); }
template <> inline Int128 decimalScaleMultiplier<Int128>(UInt32 scale) { return common::exp10_i128(scale); }

}
