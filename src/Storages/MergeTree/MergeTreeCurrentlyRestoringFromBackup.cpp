#include <Storages/MergeTree/MergeTreeCurrentlyRestoringFromBackup.h>

#include <Storages/MergeTree/MutationInfoFromBackup.h>
#include <Storages/MergeTree/calculateBlockNumbersForRestoring.h>
#include <Storages/StorageMergeTree.h>
#include <base/defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_RESTORE_TABLE;
    extern const int LOGICAL_ERROR;
}


// Increases the block number increment to allocate block numbers.
class MergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator
{
public:
    explicit BlockNumbersAllocator(
        StorageMergeTree & storage_, Poco::Logger * log_, const CurrentlyRestoringInfo & currently_restoring_info_)
        : storage(storage_), log(log_), currently_restoring_info(currently_restoring_info_)
    {
    }

    /// Allocates block numbers and recalculates `part_infos_` and `mutation_infos_`.
    void allocateBlockNumbers(
        std::vector<MergeTreePartInfo> & part_infos_,
        std::vector<MutationInfoFromBackup> & mutation_infos_,
        CheckForNoPartsReason check_for_no_parts_reason_) const
    {
        Int64 base_increment_value = getCurrentIncrementValue();
        std::optional<AllocatedBlockNumbers> allocated_block_numbers;

        /// Check the table has no existing parts if it's necessary.
        /// If a backup contains mutations the table must have no existing parts,
        /// otherwise mutations from the backup could be applied to existing parts which is wrong.
        if (check_for_no_parts_reason_ != CheckForNoPartsReason::NONE)
            checkNoPartsExist(base_increment_value, allocated_block_numbers, check_for_no_parts_reason_);

        LOG_INFO(log, "Increasing the increment to allocate block numbers for {} parts and {} mutations",
                 part_infos_.size(), mutation_infos_.size());

        /// This `do_allocate*` function will be called from calculateBlockNumbersForRestoringMergeTree() below.
        auto do_allocate_block_numbers = [&](size_t count)
        {
            auto lock = storage.lockParts();
            Int64 first = storage.increment.get(count) - count + 1;
            allocated_block_numbers.emplace(AllocatedBlockNumbers{.first = first, .count = count});
            return first;
        };

        /// Recalculate block numbers in `part_infos` and `mutation_infos`.
        calculateBlockNumbersForRestoringMergeTree(part_infos_, mutation_infos_, do_allocate_block_numbers);

        /// Check the table has no existing parts again to be sure no parts were added while we were allocating block numbers.
        if (check_for_no_parts_reason_ != CheckForNoPartsReason::NONE)
            checkNoPartsExist(base_increment_value, allocated_block_numbers, check_for_no_parts_reason_);

        LOG_INFO(log, "The increment was incremented by {} to allocate block numbers",
                 allocated_block_numbers ? allocated_block_numbers->count : 0);
    }

private:
    StorageMergeTree & storage;
    Poco::Logger * log;
    const CurrentlyRestoringInfo & currently_restoring_info;

    String getTableName() const { return storage.getStorageID().getFullTableName(); }

    struct AllocatedBlockNumbers
    {
        Int64 first = 0;
        size_t count = 0;
    };

    /// Checks that there no parts exist in the table.
    /// The function also checks that no parts will be inserted / attached soon due to a concurrent process.
    void checkNoPartsExist(Int64 base_increment_value, const std::optional<AllocatedBlockNumbers> & ignore_block_numbers, CheckForNoPartsReason reason) const
    {
        if (reason == CheckForNoPartsReason::NONE)
            return;

        LOG_INFO(log, "Checking that no parts exist in the table before restoring its data (reason: {})", magic_enum::enum_name(reason));

        /// An INSERT command first allocates a block number, then it attaches it to the table.
        /// So we do those checks twice to avoid race conditions.

        auto allocated_block_number = findAnyAllocatedBlockNumber(base_increment_value, ignore_block_numbers);

        auto part_name_in_storage = findAnyPartInStorage();
        if (part_name_in_storage)
            throwTableIsNotEmpty(reason, fmt::format("part {} exists", *part_name_in_storage));

        auto restoring_part_name = findAnyRestoringPart();
        if (restoring_part_name)
            throwTableIsNotEmpty(reason, fmt::format("concurrent RESTORE is creating part {}", *restoring_part_name));

        if (allocated_block_number)
            throwTableIsNotEmpty(reason, fmt::format("concurrent INSERT or ATTACH is using block number {}", *allocated_block_number));
    }

    /// Finds any part in the storage.
    std::optional<String> findAnyPartInStorage() const
    {
        if (storage.getTotalActiveSizeInBytes() == 0)
            return {};

        auto parts_lock = storage.lockParts();

        auto parts = storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active, MergeTreeDataPartState::PreActive}, parts_lock);
        if (parts.empty())
            return {};

        return parts.front()->name;
    }

    Int64 getCurrentIncrementValue() const
    {
        return storage.increment.value.load();
    }

    /// Finds any locked block number excluding ones we have just allocated.
    std::optional<Int64> findAnyAllocatedBlockNumber(Int64 base_increment_value, const std::optional<AllocatedBlockNumbers> & ignore_block_numbers) const
    {
        Int64 base = base_increment_value;
        Int64 current = getCurrentIncrementValue();

        if (current < base)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "The block number increment changed in a unexpected way: base={}, current={}",
                            base, current);
        }

        if (!ignore_block_numbers || !ignore_block_numbers->count)
        {
            if (current > base)
                return base + 1;
            return {};
        }

        Int64 ignore_first = ignore_block_numbers->first;
        Int64 ignore_last = ignore_block_numbers->first + ignore_block_numbers->count - 1;

        if ((ignore_first < base + 1) || (current < ignore_last))
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "The block number increment changed in a unexpected way: base={}, ignore_first={}, ignore_last={}, current={}",
                            base, ignore_first, ignore_last, current);
        }

        if (ignore_first > base + 1)
            return base + 1;

        if (current > ignore_last)
            return ignore_last + 1;

        return {};
    }

    /// Finds any part which is already restoring by a concurrent RESTORE.
    std::optional<String> findAnyRestoringPart() const;

    /// Provides an extra description for error messages to make them more detailed.
    [[noreturn]] void throwTableIsNotEmpty(CheckForNoPartsReason reason, const String & details) const
    {
        String message = fmt::format("Cannot restore the table {} because it already contains some data{}.",
                                     getTableName(), details.empty() ? "" : (" (" + details + ")"));
        if (reason == CheckForNoPartsReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED)
            message += "You can either truncate the table before restoring OR set \"allow_non_empty_tables=true\" to allow appending to "
                         "existing data in the table";
        else if (reason == CheckForNoPartsReason::RESTORING_MUTATIONS)
            message += "Mutations cannot be restored if a table is non-empty already. You can either truncate the table before "
                         "restoring OR set \"restore_mutations=false\" to restore the table without restoring its mutations";
        throw Exception(ErrorCodes::CANNOT_RESTORE_TABLE, "{}", message);
    }
};


/// Keeps information about currently restoring parts and mutations.
class MergeTreeCurrentlyRestoringFromBackup::CurrentlyRestoringInfo
{
public:
    explicit CurrentlyRestoringInfo(StorageMergeTree & storage_, Poco::Logger * log_) : storage(storage_), log(log_) { }

    /// Whether this part is being restored from a backup?
    bool containsPart(const MergeTreePartInfo & part_info) const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (entry->part_infos.contains(part_info))
                return true;
        }
        return false;
    }

    /// Whether any parts are being restored from a backup?
    bool containsAnyParts() const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (!entry->part_infos.empty())
                return true;
        }
        return false;
    }

    /// Finds any part which is already restoring.
    std::optional<String> findAnyRestoringPart() const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (!entry->part_infos.empty())
                return entry->part_infos.begin()->getPartNameAndCheckFormat(storage.format_version);
        }
        return {};
    }

    /// Whether this mutation is being restored from a backup.
    bool containsMutation(Int64 mutation_number) const
    {
        std::lock_guard lock{mutex};
        for (const auto & entry : entries)
        {
            if (entry->mutation_numbers.contains(mutation_number))
                return true;
        }
        return false;
    }

    /// Stores information about parts and mutations we're going to restore.
    /// A returned `scope_guard` is used to control how long this information should be kept.
    scope_guard addEntry(const std::vector<MergeTreePartInfo> & part_infos_, const std::vector<MutationInfoFromBackup> & mutation_infos_)
    {
        /// Make an entry to keep information about parts and mutations being restored.
        auto entry = std::make_unique<Entry>();
        auto * entry_rawptr = entry.get();

        std::copy(part_infos_.begin(), part_infos_.end(), std::inserter(entry->part_infos, entry->part_infos.end()));

        for (const auto & mutation_info : mutation_infos_)
            entry->mutation_numbers.insert(mutation_info.number);

        /// This `scope_guard` is used to remove the entry when we're done with the current RESTORE process.
        scope_guard remove_entry = [this, entry_rawptr]
        {
            LOG_INFO(log, "Removing info about currently restoring parts");
            std::lock_guard lock{mutex};
            auto it = entries.find(entry_rawptr);
            if (it != entries.end())
                entries.erase(it);
        };

        {
            /// Store the entry.
            LOG_INFO(log, "Storing info about currently restoring parts");
            std::lock_guard lock{mutex};
            entries.emplace(std::move(entry));
        }

        return remove_entry;
    }

private:
    StorageMergeTree & storage;
    Poco::Logger * const log;

    /// Represents parts and mutations restored by one RESTORE command to this table.
    struct Entry
    {
        std::set<MergeTreePartInfo> part_infos;
        std::unordered_set<Int64> mutation_numbers;
    };

    struct EntryPtrCompare
    {
        using is_transparent = void;
        template <class P, class Q>
        bool operator()(const P & left, const Q & right) const
        {
            return std::to_address(left) < std::to_address(right);
        }
    };

    std::set<std::unique_ptr<Entry>, EntryPtrCompare> entries TSA_GUARDED_BY(mutex);
    mutable std::mutex mutex;
};


std::optional<String> MergeTreeCurrentlyRestoringFromBackup::BlockNumbersAllocator::findAnyRestoringPart() const
{
    return currently_restoring_info.findAnyRestoringPart();
}


MergeTreeCurrentlyRestoringFromBackup::MergeTreeCurrentlyRestoringFromBackup(StorageMergeTree & storage_)
    : log(&Poco::Logger::get(storage_.getLogName() + " (RestorerFromBackup)"))
    , currently_restoring_info(std::make_unique<CurrentlyRestoringInfo>(storage_, log))
    , block_numbers_allocator(std::make_unique<BlockNumbersAllocator>(storage_, log, *currently_restoring_info))
{
}

MergeTreeCurrentlyRestoringFromBackup::~MergeTreeCurrentlyRestoringFromBackup() = default;

bool MergeTreeCurrentlyRestoringFromBackup::containsPart(const MergeTreePartInfo & part_info) const
{
    return currently_restoring_info->containsPart(part_info);
}

bool MergeTreeCurrentlyRestoringFromBackup::containsAnyParts() const
{
    return currently_restoring_info->containsAnyParts();
}

bool MergeTreeCurrentlyRestoringFromBackup::containsMutation(Int64 mutation_number) const
{
    return currently_restoring_info->containsMutation(mutation_number);
}

scope_guard MergeTreeCurrentlyRestoringFromBackup::allocateBlockNumbers(
    std::vector<MergeTreePartInfo> & part_infos_,
    std::vector<MutationInfoFromBackup> & mutation_infos_,
    bool check_table_is_empty_)
{
    auto check_for_no_parts_reason = getCheckForNoPartsReason(check_table_is_empty_, !mutation_infos_.empty());

    /// Allocate block numbers for restoring parts and mutations.
    block_numbers_allocator->allocateBlockNumbers(part_infos_, mutation_infos_, check_for_no_parts_reason);

    /// Store in memory the information about parts and mutations we're going to restore.
    return currently_restoring_info->addEntry(part_infos_, mutation_infos_);
}

MergeTreeCurrentlyRestoringFromBackup::CheckForNoPartsReason
MergeTreeCurrentlyRestoringFromBackup::getCheckForNoPartsReason(bool check_table_is_empty_, bool has_mutations_to_restore_)
{
    if (check_table_is_empty_)
        return CheckForNoPartsReason::NON_EMPTY_TABLE_IS_NOT_ALLOWED;
    else if (has_mutations_to_restore_)
        return CheckForNoPartsReason::RESTORING_MUTATIONS;
    else
        return CheckForNoPartsReason::NONE;
}
}
