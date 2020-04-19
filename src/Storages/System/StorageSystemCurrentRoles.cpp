#include <Storages/System/StorageSystemCurrentRoles.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Access/AccessControlManager.h>
#include <Access/User.h>
#include <Interpreters/Context.h>


namespace DB
{

NamesAndTypesList StorageSystemCurrentRoles::getNamesAndTypes()
{
    NamesAndTypesList names_and_types{
        {"role_name", std::make_shared<DataTypeString>()},
        {"admin_option", std::make_shared<DataTypeUInt8>()},
        {"is_default", std::make_shared<DataTypeUInt8>()},
    };
    return names_and_types;
}


void StorageSystemCurrentRoles::fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const
{
#if 0
    const auto & access_control = context.getAccessControlManager();
    auto ids = access_control.findAll<User>();

    size_t column_index = 0;
    auto & column_role_name = assert_cast<ColumnString &>(*res_columns[column_index++]);
    auto & column_admin_option = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();
    auto & column_is_default = assert_cast<ColumnUInt8 &>(*res_columns[column_index++]).getData();

    auto add_row = [&](const UUID & role_name, const AllowedClientHosts & allowed_hosts)
    {
        if (allowed_hosts.containsAnyHost())
        {
            column_any_host.push_back(true);
        }
        else
        {
            if (allowed_hosts.containsLocalHost())
            {
                static constexpr std::string_view str{"localhost"};
                column_host_name.insertData(str.data(), str.length());
            }

            for (const auto & ip : allowed_hosts.getAddresses())
            {
                String str = ip.toString();
                column_ip.insertData(str.data(), str.length());
            }

            for (const auto & subnet : allowed_hosts.getSubnets())
            {
                String str = subnet.toString();
                column_ip.insertData(str.data(), str.length());
            }

            for (const auto & name : allowed_hosts.getNames())
                column_host_name.insertData(name.data(), name.length());

            for (const auto & name_regexp : allowed_hosts.getNameRegexps())
                column_host_name_regexp.insertData(name_regexp.data(), name_regexp.length());

            for (const auto & like_pattern : allowed_hosts.getLikePatterns())
                column_host_name_like.insertData(like_pattern.data(), like_pattern.length());
        }

        size_t new_size = std::max(std::max(std::max(column_ip.size(), column_host_name.size()), column_host_name_regexp.size()), column_host_name_like.size());

        while (column_ip_null_map.size() < column_ip.size())
            column_ip_null_map.push_back(false);

        while (column_host_name_null_map.size() < column_host_name.size())
            column_host_name_null_map.push_back(false);

        while (column_host_name_regexp_null_map.size() < column_host_name_regexp.size())
            column_host_name_regexp_null_map.push_back(false);

        while (column_host_name_like_null_map.size() < column_host_name_like.size())
            column_host_name_like_null_map.push_back(false);

        while (column_user_name.size() < new_size)
            column_user_name.insertData(user_name.data(), user_name.length());

        while (column_any_host.size() < new_size)
            column_any_host.push_back(false);

        while (column_ip.size() < new_size)
        {
            column_ip.insertDefault();
            column_ip_null_map.push_back(true);
        }

        while (column_host_name.size() < new_size)
        {
            column_host_name.insertDefault();
            column_host_name_null_map.push_back(true);
        }

        while (column_host_name_regexp.size() < new_size)
        {
            column_host_name_regexp.insertDefault();
            column_host_name_regexp_null_map.push_back(true);
        }

        while (column_host_name_like.size() < new_size)
        {
            column_host_name_like.insertDefault();
            column_host_name_like_null_map.push_back(true);
        }
    };

    for (const auto & id : ids)
    {
        auto user = access_control.tryRead<User>(id);
        if (!user)
            continue;

        const String & user_name = user->getFullName();
        add_rows(user_name, user->allowed_client_hosts);
    }
#endif
}

}
