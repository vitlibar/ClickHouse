#pragma once

#include <ext/shared_ptr_helper.h>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{
class Context;

/// Implements `system.granted_roles` table, which allows you to get information about granted roles.
class StorageSystemGrantedRoles final : public ext::shared_ptr_helper<StorageSystemGrantedRoles>, public IStorageSystemOneBlock<StorageSystemGrantedRoles>
{
public:
    std::string getName() const override { return "SystemGrantedRoles"; }
    static NamesAndTypesList getNamesAndTypes();

protected:
    friend struct ext::shared_ptr_helper<StorageSystemGrantedRoles>;
    using IStorageSystemOneBlock::IStorageSystemOneBlock;
    void fillData(MutableColumns & res_columns, const Context & context, const SelectQueryInfo &) const override;
};

}
