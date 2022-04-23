import pytest
import os.path
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV


cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml", "configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    macros={"replica": "node1", "shard": "shard1"},
    with_zookeeper=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml", "configs/backups_disk.xml"],
    external_dirs=["/backups/"],
    macros={"replica": "node2", "shard": "shard1"},
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
        node1.query("DROP TABLE IF EXISTS tbl ON CLUSTER 'cluster' NO DELAY")
    finally:
        cluster.shutdown()


def create_table(instance = None):
    on_cluster_clause = "" if instance else "ON CLUSTER 'cluster'"
    instance_to_execute = instance if instance else node1
    instance_to_execute.query(
        "CREATE TABLE tbl " + on_cluster_clause + " ("
            "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', '{replica}')"
        "ORDER BY x"
    )


def drop_table(instance = None):
    on_cluster_clause = "" if instance else "ON CLUSTER 'cluster'"
    instance_to_execute = instance if instance else node1
    instance_to_execute.query(f"DROP TABLE tbl {on_cluster_clause} NO DELAY")


def insert_data(instance = None):
    instance1_to_execute = instance if instance else node1
    instance2_to_execute = instance if instance else node2
    instance1_to_execute.query("INSERT INTO tbl VALUES (1, 'Don''t')")
    instance2_to_execute.query("INSERT INTO tbl VALUES (2, 'count')")
    instance1_to_execute.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (3, 'your')")
    instance2_to_execute.query("INSERT INTO tbl SETTINGS async_insert=true VALUES (4, 'chickens')")


backup_id_counter = 0


def new_backup_name():
    global backup_id_counter
    backup_id_counter += 1
    return f"Disk('backups', '{backup_id_counter}.zip')"


def get_path_to_backup(instance, backup_name):
    return os.path.join(
        instance.path,
        "backups",
        backup_name.removeprefix("Disk('backups', '").removesuffix("')"),
    )


def test_replicated_table():
    create_table()
    insert_data()

    backup_name = new_backup_name()

    # Make backup on node 1.
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS replica=1")

    # Drop table on both nodes.
    drop_table()

    # Restore from backup on node2.
    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name}")

    assert node2.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )

    assert node1.query("SELECT * FROM tbl ORDER BY x") == TSV(
        [[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]]
    )


def test2():
    node1.query(
        "CREATE TABLE tbl ("
            "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', 'ra')"
        "ORDER BY x")

    node2.query(
        "CREATE TABLE tbl2 ("
            "x UInt8, y String"
        ") ENGINE=ReplicatedMergeTree('/clickhouse/tables/tbl/', 'ra')"
        "ORDER BY x")

    insert_data(node1)

    print ('node1-parts')
    print (node1.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl'"))
    print ('node2-parts')
    print (node2.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl2'"))

    print ('optimize')
    print (node1.query("OPTIMIZE TABLE tbl FINAL"))
    print ('node1-parts')
    print (node1.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl'"))
    print ('node2-parts')
    print (node2.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl2'"))

    print ('node1')
    print (node1.query("SELECT * FROM tbl ORDER BY x"))
    print ('node2')
    print (node2.query("SELECT * FROM tbl2 ORDER BY x"))

    print ('detach')
    node1.query("ALTER TABLE tbl DETACH PART 'all_2_2_0'")

    print ('node1-parts')
    print (node1.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl'"))
    print ('node2-parts')
    print (node2.query("SELECT name, active, visible, rows, bytes_on_disk, disk_name, path FROM system.parts WHERE table='tbl2'"))

    assert False


def test3():
    node1.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','sa','ra')", settings={'allow_experimental_database_replicated': True})
    node2.query("CREATE DATABASE mydb2 ENGINE=Replicated('/clickhouse/path/','sa','rb')", settings={'allow_experimental_database_replicated': True})

    node1.query("CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x")
    print (node2.query("EXISTS mydb2.tbl"))
    print (node2.query("SHOW CREATE TABLE mydb2.tbl"))

    node2.query("INSERT INTO mydb2.tbl VALUES (2, 'count')")
    print (node1.query("SELECT * FROM mydb.tbl ORDER BY x"))

    print ('MergeTree')
    node1.query("CREATE TABLE mydb.tbla(x UInt8, y String) ENGINE=MergeTree ORDER BY x")
    print (node2.query("EXISTS mydb2.tbla"))
    print (node2.query("SHOW CREATE TABLE mydb2.tbla"))
    node2.query("INSERT INTO mydb2.tbla VALUES (2, 'count')")
    print (node1.query("SELECT * FROM mydb.tbla ORDER BY x"))

    assert False


def test4():
    node1.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','shard1','{replica}')", settings={'allow_experimental_database_replicated': True})
    print (node1.query("SHOW CREATE DATABASE mydb"))
    
    assert False


def test5():
    node1.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','sa','ra')", settings={'allow_experimental_database_replicated': True})
    node2.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','sb','rb')", settings={'allow_experimental_database_replicated': True})
    node1.query("CREATE TABLE mydb.tbl(x UInt8, y String) ENGINE=ReplicatedMergeTree ORDER BY x")
    print (node1.query("SHOW CREATE TABLE mydb.tbl"))
    print (node2.query("SHOW CREATE TABLE mydb.tbl"))

    assert False


def test6():
    node1.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','shard1','{replica}')", settings={'allow_experimental_database_replicated': True})
    node1.query("CREATE TABLE mydb.tbl (`x` UInt8, `y` String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/a109341b-aa12-4328-a4e5-8c209ec4fe80/{shard}', '{replica}') ORDER BY x SETTINGS index_granularity = 8192")
    print (node1.query("SHOW CREATE TABLE mydb.tbl"))

    assert False


def test7():
    node1.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','s1','r1')", settings={'allow_experimental_database_replicated': True})
    node2.query("CREATE DATABASE mydb ENGINE=Replicated('/clickhouse/path/','s2','r2')", settings={'allow_experimental_database_replicated': True})
    node1.query("CREATE TABLE mydb.tbl (`x` UInt8, `y` String) ENGINE = ReplicatedMergeTree('/clickhouse/tbl/{shard}', '{replica}') ORDER BY x")
    node2.query("CREATE TABLE mydb.tbl (`x` UInt8, `y` String) ENGINE = ReplicatedMergeTree('/clickhouse/tbl/{shard}', '{replica}') ORDER BY x")
    print (node1.query("SHOW CREATE TABLE mydb.tbl"))

    assert False


def test_different_tables_with_same_names_on_nodes():
    node1.query("CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x")
    node2.query("CREATE TABLE tbl (`w` Int64) ENGINE = MergeTree ORDER BY w")

    node1.query("INSERT INTO tbl VALUES (1, 'Don''t'), (2, 'count'), (3, 'your'), (4, 'chickens')")
    node2.query("INSERT INTO tbl VALUES (-333), (-222), (-111), (0), (111)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl ON CLUSTER 'cluster' TO {backup_name} SETTINGS allow_storing_multiple_replicas = true")

    node1.query("DROP TABLE tbl")
    node2.query("DROP TABLE tbl")

    node2.query(f"RESTORE TABLE tbl ON CLUSTER 'cluster' FROM {backup_name} SETTINGS allow_using_multiple_replicas_in_backup = true")

    assert node1.query("SELECT * FROM tbl") == TSV([[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]])
    assert node2.query("SELECT * FROM tbl") == TSV([-333, -222, -111, 0, 111])


def test_different_tables_on_nodes():
    node1.query("CREATE TABLE tbl (`x` UInt8, `y` String) ENGINE = MergeTree ORDER BY x")
    node2.query("CREATE TABLE tbl2 (`w` Int64) ENGINE = MergeTree ORDER BY w")

    node1.query("INSERT INTO tbl VALUES (1, 'Don''t'), (2, 'count'), (3, 'your'), (4, 'chickens')")
    node2.query("INSERT INTO tbl2 VALUES (-333), (-222), (-111), (0), (111)")

    backup_name = new_backup_name()
    node1.query(f"BACKUP TABLE tbl, TABLE tbl2 ON CLUSTER 'cluster' TO {backup_name} SETTINGS allow_storing_multiple_replicas = true")

    node1.query("DROP TABLE tbl")
    node2.query("DROP TABLE tbl2")

    node2.query(f"RESTORE TABLE tbl, TABLE tbl2 ON CLUSTER 'cluster' FROM {backup_name} SETTINGS allow_using_multiple_replicas_in_backup = true")

    assert node1.query("SELECT * FROM tbl") == TSV([[1, "Don\\'t"], [2, "count"], [3, "your"], [4, "chickens"]])
    assert node2.query("SELECT * FROM tbl2") == TSV([-333, -222, -111, 0, 111])
