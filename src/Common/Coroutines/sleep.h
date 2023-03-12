#pragma once

#include <Common/Coroutines/Task_fwd.h>


namespace DB::Coroutine
{

/// Waits for specified time as a coroutine.
/// Usage:
///     co_await Coroutine::sleepForSeconds(5);
///
/// Note:
/// `Coroutine::sleepForSeconds(5).syncRun()` is the same as just
/// `::sleepForSeconds(5)`
///
Task<> sleepForSeconds(uint64_t seconds);
Task<> sleepForMilliseconds(uint64_t milliseconds);
Task<> sleepForMicroseconds(uint64_t microseconds);

}
