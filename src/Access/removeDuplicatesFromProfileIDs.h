#pragma once

#include <Core/UUID.h>


namespace DB
{

/// Removes duplicates from a vector of profile IDs.
/// This function keeps the latest elements and does't change the order of elements.
void removeDuplicatesFromProfileIDs(std::vector<UUID> & ids);

}
