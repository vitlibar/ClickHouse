#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `system.allowed_hosts` table, which allows you to get information about elements of allowed client hosts.
class StorageSystemAllowedHosts final : public ext::shared_ptr_helper<StorageSystemAllowedHosts>, public IStorageSystemOneBlock<StorageSystemAllowedHosts>
{
public:
    std::string getName() const override { return "SystemAllowedHosts"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemAllowedHosts>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
