#include <gtest/gtest.h>

#include <sstream>
#include <iostream>


TEST(StringStream, SizeLimit)
{
    std::string megabyte(1024*1024, 'a');

    size_t two_gigabytes = 2 * 1024;
    size_t four_gigabytes = 4 * 1024;
    size_t five_gigabytes = 5 * 1024;

    std::stringstream ss;

    size_t step = 0;
    for (; step != five_gigabytes; ++step)
    {
        if (step == two_gigabytes || step == two_gigabytes - 1 || step == two_gigabytes + 1 || step == four_gigabytes
            || step == four_gigabytes - 1 || step == four_gigabytes + 1 || step == five_gigabytes - 1)
        {
            std::cout << "step #" << step << std::endl;
        }
        ss << megabyte;
        if (!ss.good())
            break;
    }

    std::stringstream msg_stream;
    msg_stream << "Stopped on step #" << step << ", tellp=" << ss.tellp() << ", good=" << ss.good() << ", bad=" << ss.bad()
           << ", fail=" << ss.fail() << ", eof=" << ss.eof();
    std::cout << msg_stream.str() << std::endl;
    EXPECT_EQ("Stopped on step #5120, tellp=5368709120, good=1, bad=0, fail=0, eof=0", msg_stream.str());
}
