#include <Storages/TimeSeries/checkTimeSeriesTargetTable.h>

#include <Core/Field.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace ErrorCodes
{
    extern const int THERE_IS_NO_COLUMN;
}


void checkTimeSeriesTargetTable(
    const StorageID & target_table_id,
    const ColumnsDescription & target_table_columns,
    ViewTarget::Kind target_kind,
    const TimeSeriesSettings & time_series_settings)
{
    auto check_column = [&](std::string_view column_name)
    {
        if (!target_table_columns.tryGet(String(column_name)))
            throw Exception(ErrorCodes::THERE_IS_NO_COLUMN, "{}: Column {} is required for the {} table used by TimeSeries table engine",
                            target_table_id.getNameForLogs(), column_name, target_kind);
    };

    switch (target_kind)
    {
        case ViewTarget::Samples:
        {
            check_column(TimeSeriesColumnNames::ID);
            check_column(TimeSeriesColumnNames::Timestamp);
            check_column(TimeSeriesColumnNames::Value);
            break;
        }

        case ViewTarget::Tags:
        {
            check_column(TimeSeriesColumnNames::MetricName);

            const Map & tags_to_columns = time_series_settings[TimeSeriesSetting::tags_to_columns];
            for (const auto & tag_name_and_column_name : tags_to_columns)
            {
                const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
                const auto & column_name = tuple.at(1).safeGet<String>();
                check_column(column_name);
            }

            check_column(TimeSeriesColumnNames::Tags);
            break;
        }

        case ViewTarget::Metrics:
        {
            check_column(TimeSeriesColumnNames::MetricFamilyName);
            check_column(TimeSeriesColumnNames::Type);
            check_column(TimeSeriesColumnNames::Unit);
            check_column(TimeSeriesColumnNames::Help);
            break;
        }

        default:
            UNREACHABLE();
    }
}

}
