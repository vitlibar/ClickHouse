#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesGroupValuesFromGrid.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeNullable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}


namespace
{
    template <typename ValueType>
    AggregateFunctionPtr createWithValueType(const String & name, const DataTypes & argument_types)
    {
        if (name == AggregateFunctionTimeSeriesGroupValuesFromGrid<ValueType, false>::name)
            return std::make_shared<AggregateFunctionTimeSeriesGroupValuesFromGrid<ValueType, false>>(argument_types);
        else if (name == AggregateFunctionTimeSeriesGroupValuesFromGrid<ValueType, true>::name)
            return std::make_shared<AggregateFunctionTimeSeriesGroupValuesFromGrid<ValueType, true>>(argument_types);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected name {}", name);
    }

    AggregateFunctionPtr createAggregateFunctionTimeseriesGroupValuesFromGrid(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        if (argument_types[0]->getTypeId() != TypeIndex::Array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());

        const auto & nullable_type = typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType();

        if (nullable_type->getTypeId() != TypeIndex::Nullable)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());

        const auto & value_type = typeid_cast<const DataTypeNullable *>(nullable_type.get())->getNestedType();

        AggregateFunctionPtr res;
        if (value_type->getTypeId() == TypeIndex::Float64)
        {
            res = createWithValueType<Float64>(name, argument_types);
        }
        else if (value_type->getTypeId() == TypeIndex::Float32)
        {
            res = createWithValueType<Float32>(name, argument_types);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects one argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());
        }

        return res;
    }
}

void registerAggregateFunctionTimeseriesGroupValuesFromGrid(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesGroupValuesFromGrid", createAggregateFunctionTimeseriesGroupValuesFromGrid);
    factory.registerFunction("timeSeriesGroupValuesFromGridOrNull", createAggregateFunctionTimeseriesGroupValuesFromGrid);
}

}
