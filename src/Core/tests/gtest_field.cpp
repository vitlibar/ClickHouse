#include <gtest/gtest.h>
#include <Core/Field.h>

using namespace DB;

GTEST_TEST(Field, GetAsBool)
{
    /// false to Field
    {
        Field f{false};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<bool>(), false);
        ASSERT_EQ(f.getAsBool(), false);
    }

    {
        Field f;
        f = false;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<bool>(), false);
        ASSERT_EQ(f.getAsBool(), false);
    }

    /// true to Field
    {
        Field f{true};
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<bool>(), true);
        ASSERT_EQ(f.getAsBool(), true);
    }

    {
        Field f;
        f = true;
        ASSERT_EQ(f.getType(), Field::Types::Bool);
        ASSERT_EQ(f.get<bool>(), true);
        ASSERT_EQ(f.getAsBool(), true);
    }

    /// 0u to Field
    {
        Field f{0u};
        ASSERT_EQ(f.getType(), Field::Types::UInt64);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.get<Int64>(), 0);
        ASSERT_EQ(f.getAsBool(), false);
    }

    {
        Field f;
        f = 0u;
        ASSERT_EQ(f.getType(), Field::Types::UInt64);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.get<Int64>(), 0);
        ASSERT_EQ(f.getAsBool(), false);
    }

    /// 1u to Field
    {
        Field f{1u};
        ASSERT_EQ(f.getType(), Field::Types::UInt64);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.get<Int64>(), 1);
        ASSERT_EQ(f.getAsBool(), true);
    }

    {
        Field f;
        f = 1u;
        ASSERT_EQ(f.getType(), Field::Types::UInt64);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.get<Int64>(), 1);
        ASSERT_EQ(f.getAsBool(), true);
    }

    /// 0 to Field
    {
        Field f{0};
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 0);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.getAsBool(), false);
    }

    {
        Field f;
        f = 0;
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 0);
        ASSERT_EQ(f.get<UInt64>(), 0);
        ASSERT_EQ(f.getAsBool(), false);
    }

    /// 1 to Field
    {
        Field f{1};
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 1);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.getAsBool(), true);
    }

    {
        Field f;
        f = 1;
        ASSERT_EQ(f.getType(), Field::Types::Int64);
        ASSERT_EQ(f.get<Int64>(), 1);
        ASSERT_EQ(f.get<UInt64>(), 1);
        ASSERT_EQ(f.getAsBool(), true);
    }
}


GTEST_TEST(Field, Move)
{
    Field f;

    f = Field{String{"Hello, world (1)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (1)");
    f = Field{String{"Hello, world (2)"}};
    ASSERT_EQ(f.get<String>(), "Hello, world (2)");
    f = Field{Array{Field{String{"Hello, world (3)"}}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (3)");
    f = String{"Hello, world (4)"};
    ASSERT_EQ(f.get<String>(), "Hello, world (4)");
    f = Array{Field{String{"Hello, world (5)"}}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (5)");
    f = Array{String{"Hello, world (6)"}};
    ASSERT_EQ(f.get<Array>()[0].get<String>(), "Hello, world (6)");
}
