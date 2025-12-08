#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/PrometheusQueryEvaluationSettings.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultType.h>


namespace DB::PrometheusQueryToSQL
{

ColumnsDescription getResultColumns(const PQT & promql_tree, const PrometheusQueryEvaluationSettings & settings)
{
    ColumnsDescription columns;

    auto result_type = getResultType(promql_tree, settings);
    auto timestamp_type = getResultTimestampType(settings);
    auto scalar_type = getResultScalarType(settings);

    switch (result_type)
    {
        case ResultType::SCALAR:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, scalar_type});
            break;
        }
        case ResultType::STRING:
        {
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, std::make_shared<DataTypeString>()});
            break;
        }
        case ResultType::INSTANT_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    ColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(ColumnDescription{ColumnNames::Timestamp, timestamp_type});
            columns.add(ColumnDescription{ColumnNames::Value, scalar_type});
            break;
        }
        case ResultType::RANGE_VECTOR:
        {
            columns.add(
                ColumnDescription{
                    ColumnNames::Tags,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                        DataTypes{std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>()}))});
            columns.add(
                ColumnDescription{
                    ColumnNames::TimeSeries,
                    std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{timestamp_type, scalar_type}))});
            break;
        }
    }
    return columns;
}

}
