#pragma once

#include <Common/Logger.h>
#include <Core/UUID.h>
#include <unordered_map>


namespace DB
{
class AccessControl;
enum class AccessEntityType : uint8_t;
struct IAccessEntity;
using AccessEntityPtr = std::shared_ptr<const IAccessEntity>;
class AccessRightsElements;
class IAccessStorage;
class IBackup;
using BackupPtr = std::shared_ptr<const IBackup>;
class IBackupEntry;
using BackupEntryPtr = std::shared_ptr<const IBackupEntry>;
struct RestoreSettings;
enum class RestoreAccessCreationMode : uint8_t;


/// Makes a backup of access entities of a specified type.
std::pair<String, BackupEntryPtr> makeBackupEntryForAccess(
    const std::vector<std::pair<UUID, AccessEntityPtr>> & access_entities,
    const String & data_path_in_backup,
    size_t counter,
    const AccessControl & access_control);

/// Restores access entities from a backup.
void restoreAccessEntitiesFromBackup(
    IAccessStorage & access_storage,
    const std::vector<std::pair<UUID, AccessEntityPtr>> & entities,
    const std::vector<std::pair<UUID, AccessEntityPtr>> & dependants,
    const RestoreSettings & restore_settings);


/// Loads access entities from a backup and prepares them for insertion into an access storage.
class AccessRestorerFromBackup
{
public:
    AccessRestorerFromBackup(const BackupPtr & backup_, const RestoreSettings & restore_settings_);
    ~AccessRestorerFromBackup();

    /// Adds a data path to loads access entities from.
    void addDataPath(const String & data_path_in_backup, bool dependants_only = false);

    /// Loads access entities from the backup.
    void loadFromBackup();

    /// Checks that the current user can do restoring.
    /// Function loadFromBackup() must be called before that.
    AccessRightsElements getRequiredAccess() const;

    /// Generates random IDs for access entities we're restoring to insert them into an access storage;
    /// and finds IDs of existing access entities which are used as dependencies. 
    void generateRandomIDsAndResolveDependencies(const AccessControl & access_control);

    /// Returns access entities prepared for insertion into an access storage and new random UUIDs generated for those access entities.
    /// Both functions loadFromBackup() and generateRandomIDsAndResolveDependencies() must be called before that.
    std::vector<std::pair<UUID, AccessEntityPtr>> getEntities(const String & data_path_in_backup) const;

    /// Returns dependants of the access entities we're going to restore.
    /// Dependants are access entities which exist already and they should be updated after restoring.
    /// For example, if there were a role granted to a user: `CREATE USER user1; CREATE ROLE role1; GRANT role1 TO user1`,
    /// and we're restoring only role `role1` because user `user1` already exists,
    /// then user `user1` should be modified after restoring role `role1` to add this grant `GRANT role1 TO user1`.
    std::vector<std::pair<UUID, AccessEntityPtr>> getDependants(const String & data_path_in_backup) const;

private:
    const BackupPtr backup;
    const RestoreAccessCreationMode creation_mode;
    const bool skip_unresolved_dependencies;
    const bool update_dependants;
    const LoggerPtr log;

    /// Whether loadFromBackup() finished.
    bool loaded = false;

    /// Whether generateRandomIDsAndResolveDependencies() finished.
    bool ids_assigned = false;

    std::vector<std::pair<String, bool /* dependants_only */>> data_paths_in_backup;
    String data_path_with_entities_to_restore;

    /// Information about an access entity loaded from the backup.
    struct EntityInfo
    {
        UUID id;
        String name;
        AccessEntityType type;

        AccessEntityPtr entity; /// Can be nullptr if `restore=false`.

        /// Index in `data_paths_in_backup`.
        size_t data_path_index = 0;

        /// Whether we're going to restore this entity.
        /// For example,
        /// in case of `RESTORE TABLE system.roles` this flag is true for all the roles loaded from the backup, and
        /// in case of `RESTORE ALL` this flag is always true.
        bool restore = false;

        /// Whether this entity info was added as a dependency of another entity which we're going to restore.
        /// For example, if we're going to restore the following user: `CREATE USER user1 DEFAULT ROLE role1, role2 SETTINGS PROFILE profile1, profile2`
        /// then `restore=true` for `user1` and `is_dependency=true` for `role1`, `role2`, `profile1`, `profile2`.
        /// Flags `restore` and `is_dependency` both can be set at the same time.
        bool is_dependency = false;

        /// Whether this entity info is a dependant of another entity which we're going to restore.
        /// For example, if we're going to restore role `role1` and there is also the following user stored in the backup:
        /// `CREATE USER user1 DEFAULT ROLE role1`, then `is_dependant=true` for `user1`.
        /// This flags is set by generateRandomIDsAndResolveDependencies().
        bool is_dependant = false;

        /// New UUID for this entity - either randomly generated or copied from an existing entity.
        /// This UUID is assigned by generateRandomIDsAndResolveDependencies().
        std::optional<UUID> new_id;
    };

    std::unordered_map<UUID, EntityInfo> entity_infos;
};

}
