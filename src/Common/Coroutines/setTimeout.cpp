#include <Common/Coroutines/setTimeout.h>

#include <Common/Coroutines/Task.h>


namespace DB::ErrorCodes
{
    extern const int COROUTINE_TASK_TIMEOUT;
}


namespace DB::Coroutine
{

namespace details
{
    std::exception_ptr makeTimeoutException(TimeUnit time_unit, uint64_t count)
    {
        return std::make_exception_ptr(Exception{
            ErrorCodes::COROUTINE_TASK_TIMEOUT,
            "A coroutine task was cancelled because the timeout {} {} has reached",
            count,
            getTimeUnitsName(time_unit)});
    }
}

template Task<> setTimeoutInSeconds(Task<> task, uint64_t seconds);
template Task<> setTimeoutInMilliseconds(Task<> task, uint64_t milliseconds);
template Task<> setTimeoutInMicroseconds(Task<> task, uint64_t microseconds);

}
