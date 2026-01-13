#include <Core/DecimalFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CLOCK_GETTIME;
}


namespace DecimalUtils
{

DateTime64 getCurrentDateTime64(UInt32 scale)
{
    timespec spec{};
    if (clock_gettime(CLOCK_REALTIME, &spec))
        throw ErrnoException(ErrorCodes::CANNOT_CLOCK_GETTIME, "Cannot clock_gettime");

    DateTime64 time = spec.tv_sec * scaleMultiplier<Int64>(scale);

    if (scale < 9)
        time += spec.tv_nsec / scaleMultiplier<Int64>(9 - scale);
    else if (scale == 9)
        time += spec.tv_nsec;
    else
        time += spec.tv_nsec * scaleMultiplier<Int64>(scale - 9);

    return time;
}

}

}
