#pragma once


namespace DB
{

struct RestoreWithReplaceMode
{
    bool replace_table_if_exists = false;
    bool replace_database_if_exists = false;
};

}
