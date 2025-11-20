#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{

ColumnsDescription getResultColumns(ResultType result_type, const DataTypePtr & timestamp_type, const DataTypePtr & scalar_type)
{
    ColumnsDescription columns;

    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Scalar, timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Scalar, scalar_type});
            break;
        }
        case ResultType::STRING:
        {
            columns.add(ColumnDescription{TimeSeriesColumnNames::Scalar, timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::String, std::make_shared<DataTypeString>()});
            break;
        }
        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{TimeSeriesColumnNames::Value, scalar_type});
            break;
        }
        case ResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    TimeSeriesColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type}))});
            break;
        }
    }
    return columns;
}

ColumnsDescription getResultColumns(ResultType result_type, const ConverterContext & context)
{
    return getResultColumns(result_type, context.timestamp_type, context.scalar_type);
}

}
