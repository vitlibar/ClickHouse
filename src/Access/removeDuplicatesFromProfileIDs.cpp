#include <Access/removeDuplicatesFromProfileIDs.h>


namespace DB
{

void removeDuplicatesFromProfileIDs(std::vector<UUID> & ids)
{
    auto begin = ids.begin();
    auto end = ids.end();
    auto new_begin = end;

    for (auto current = end; current != begin;)
    {
        --current;
        if (std::find(new_begin, end, *current) == end)
        {
            --new_begin;
            if (new_begin != current)
                *new_begin = *current;
        }
    }

    ids.erase(begin, new_begin);
}

}
