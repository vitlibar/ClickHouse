import time
import pytest
from helpers.cluster import ClickHouseCluster
from helpers.client import QueryRuntimeException

cluster = ClickHouseCluster(__file__)
ch1 = cluster.add_instance('ch1', config_dir="configs", with_zookeeper=True)
ch2 = cluster.add_instance('ch2', config_dir="configs", with_zookeeper=True)
ch3 = cluster.add_instance('ch3', config_dir="configs", with_zookeeper=True)

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_access_control_on_cluster():
    ch1.query("CREATE USER Alex ON CLUSTER 'cluster'")
    assert ch1.query("SHOW CREATE USER Alex") == "CREATE USER Alex\n"
    assert ch2.query("SHOW CREATE USER Alex") == "CREATE USER Alex\n"
    assert ch3.query("SHOW CREATE USER Alex") == "CREATE USER Alex\n"

    ch2.query("GRANT ON CLUSTER 'cluster' SELECT ON *.* TO Alex")
    assert ch1.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"
    assert ch2.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"
    assert ch3.query("SHOW GRANTS FOR Alex") == "GRANT SELECT ON *.* TO Alex\n"

    ch3.query("REVOKE ON CLUSTER 'cluster' SELECT ON *.* FROM Alex")
    assert ch1.query("SHOW GRANTS FOR Alex") == ""
    assert ch2.query("SHOW GRANTS FOR Alex") == ""
    assert ch3.query("SHOW GRANTS FOR Alex") == ""

    ch2.query("DROP USER Alex ON CLUSTER 'cluster'")
    assert "There is no user `Alex`" in ch1.query_and_get_error("SHOW CREATE USER Alex")
    assert "There is no user `Alex`" in ch2.query_and_get_error("SHOW CREATE USER Alex")
    assert "There is no user `Alex`" in ch3.query_and_get_error("SHOW CREATE USER Alex")


def test_row_policies_on_cluster():
    ch1.query("CREATE POLICY pol1 ON mydb.mytable ON CLUSTER 'cluster'")
    ch1.query("CREATE POLICY pol2 ON mydb.mytable ON CLUSTER 'cluster'")
    ch1.query("CREATE POLICY pol3 ON mydb.mytable ON CLUSTER 'cluster'")
    ch1.query("CREATE POLICY pol4 ON CLUSTER 'cluster' ON mydb.mytable")
    ch1.query("CREATE POLICY pol5 ON CLUSTER 'cluster' ON mydb.mytable")
    ch1.query("CREATE POLICY pol6 ON CLUSTER 'cluster' ON mydb.mytable")
    ch1.query("CREATE POLICY pol7 ON mydb.mytable ON CLUSTER 'cluster'")
    ch1.query("CREATE POLICY pol8 ON mydb.mytable2 ON CLUSTER 'cluster'")

    ch2.query("DROP POLICY pol1 ON mydb.mytable ON CLUSTER 'cluster'")
    ch2.query("DROP POLICY pol2 ON CLUSTER 'cluster' ON mydb.mytable")
    ch2.query("DROP POLICY pol3, pol4 ON mydb.mytable ON CLUSTER 'cluster'")
    ch2.query("DROP POLICY pol5, pol6 ON CLUSTER 'cluster' ON mydb.mytable")
    ch2.query("DROP POLICY pol7 ON mydb.mytable, pol8 ON mydb.mytable2 ON CLUSTER 'cluster'")
