#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeSeriesCoalesceGridValues.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/CurrentThread.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_time_series_aggregate_functions;
    extern const SettingsBool allow_experimental_time_series_table;
}


namespace
{
    using Mode = AggregateFunctionTimeSeriesCoalesceGridValuesMode;

    Mode parseParameterMode(std::string_view function_name, std::string_view parameter_name, const Field & parameter_field)
    {
        String string_value;
        if (!parameter_field.tryGet(string_value))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of {} parameter for aggregate function {}",
                parameter_field.getTypeName(), parameter_name, function_name);
        }

        if (string_value == "any")
            return Mode::kAny;
        else if (string_value == "nan")
            return Mode::kNaN;
        else if (string_value == "throw")
            return Mode::kThrow;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse {} parameter for aggregate function {}", parameter_name, function_name);
    }

    template <typename ValueType>
    AggregateFunctionPtr createWithValueType(std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector, const DataTypes & argument_types, Mode mode)
    {
        return std::make_shared<AggregateFunctionTimeSeriesCoalesceGridValues<ValueType>>(tags_collector, argument_types, mode);
    }

    AggregateFunctionPtr createAggregateFunctionTimeSeriesCoalesceGridValues(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        if (settings && (*settings)[Setting::allow_experimental_time_series_aggregate_functions] == 0 && (*settings)[Setting::allow_experimental_time_series_table] == 0)
            throw Exception(
                ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION,
                "Aggregate function {} is experimental and disabled by default. Enable it with setting allow_experimental_time_series_aggregate_functions",
                name);

        std::shared_ptr<const ContextTimeSeriesTagsCollector> tags_collector;
        if (CurrentThread::isInitialized())
            tags_collector = CurrentThread::get().getQueryContext()->getTimeSeriesTagsCollector();

        if (parameters.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires 1 parameter: mode", name);

        Mode mode = parseParameterMode(name, "mode", parameters[0]);

        size_t num_args = argument_types.size();
        if (num_args < 1 || num_args > 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires 1..2 arguments: {}(mode)(values [, group])", name, name);

        if (argument_types[0]->getTypeId() != TypeIndex::Array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects the first argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());

        if ((argument_types.size() == 2) && (argument_types[1]->getTypeId() != TypeIndex::UInt64))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects the second argument of type UInt64, got type {}",
                name, argument_types[1]->getName());

        auto element_type = typeid_cast<const DataTypeArray *>(argument_types[0].get())->getNestedType();
        auto value_type = removeNullable(element_type);

        AggregateFunctionPtr res;
        if (value_type->getTypeId() == TypeIndex::Float64)
        {
            res = createWithValueType<Float64>(tags_collector, argument_types, mode);
        }
        else if (value_type->getTypeId() == TypeIndex::Float32)
        {
            res = createWithValueType<Float32>(tags_collector, argument_types, mode);
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} expects the first argument of type Array(Nullable(floating-point)), got type {}",
                name, argument_types[0]->getName());
        }

        return res;
    }
}

void registerAggregateFunctionTimeSeriesCoalesceGridValues(AggregateFunctionFactory & factory)
{
    factory.registerFunction("timeSeriesCoalesceGridValues", createAggregateFunctionTimeSeriesCoalesceGridValues);
}

}
