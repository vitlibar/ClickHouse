import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import os
import re
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir="configs")

query_from_system_quotas = "SELECT * FROM system.quotas ORDER BY name";

query_from_system_quota_usage = "SELECT id, key, duration, "\
                                 "queries, errors, result_rows, result_bytes, read_rows, read_bytes "\
                                 "FROM system.quota_usage ORDER BY id, key, duration";

def system_quotas():
    return instance.query(query_from_system_quotas).rstrip('\n')

def system_quota_usage():
    return instance.query(query_from_system_quota_usage).rstrip('\n')


def copy_quota_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/quota.xml')
    if reload_immediately:
       instance.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        
        instance.query("CREATE TABLE test_table(x UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test_table SELECT number FROM numbers(50)")

        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def reset_usage_info():
    try:
        yield
    finally:
        copy_quota_xml('simpliest.xml') # To reset usage info.
        copy_quota_xml('normal_limits.xml')


def test_quota_from_users_xml():
    assert instance.query("SELECT currentQuota()") == "myQuota\n"
    assert instance.query("SELECT currentQuotaID()") == "e651da9c-a748-8703-061a-7e5e5096dae7\n"
    assert instance.query("SELECT currentQuotaKey()") == "default\n"
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"

    instance.query("SELECT COUNT() from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t51\t208\t50\t200"


def test_simpliest_quota():
    # Simpliest quota doesn't even track usage.
    copy_quota_xml('simpliest.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\'\n"
    assert instance.query("SHOW QUOTA") == "myQuota KEY=\\'default\\'\n"
    assert instance.query("SHOW QUOTAS") == "myQuota KEY=\\'default\\'\n"

    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N"
    assert instance.query("SHOW QUOTA") == "myQuota KEY=\\'default\\'\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.


def test_tracking_quota():
    # Now we're tracking usage.
    copy_quota_xml('tracking.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"
    assert instance.query("SHOW CREATE QUOTA CURRENT") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' SET TRACKING FOR INTERVAL 1 YEAR\n"
    assert instance.query("SHOW QUOTA CURRENT") == "myQuota KEY=\\'default\\' INTERVAL=[2019-01-01 00:10:48 .. 2020-01-01 06:00:00] queries=0 errors=0 result rows=0 result bytes=0 read rows=0 read bytes=0 execution time=0\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.

    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"

    instance.query("SELECT COUNT() from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t51\t208\t50\t200"


def test_exceed_quota():
    # Change quota, now the limits are tiny so we will exceed the quota.
    copy_quota_xml('tiny_limits.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1]\t[1]\t[1]\t[0]\t[1]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t2\t1\t0\t1\t51\t1\t208\t0\t50\t1\t200\t0\t0"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' SET MAX QUERIES = 1, MAX ERRORS = 1, MAX RESULT ROWS = 1, MAX READ ROWS = 1 FOR INTERVAL 1 YEAR\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.

    assert re.search("Quota.*has\ been\ exceeded", instance.query_and_get_error("SELECT * from test_table"))
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t1\t0\t0\t50\t0"

    # Change quota, now the limits are enough to execute queries.
    copy_quota_xml('normal_limits.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t1\t0\t0\t50\t0"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t3\t1000\t1\t0\t51\t0\t208\t0\t50\t1000\t200\t0\t0"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' SET MAX QUERIES = 1000, MAX READ ROWS = 1000 FOR INTERVAL 1 YEAR\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t1\t50\t200\t100\t200"


def test_add_remove_interval():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    # Add interval.
    copy_quota_xml('two_intervals.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952,63113904]\t[0,1]\t[1000,0]\t[0,0]\t[0,0]\t[0,30000]\t[1000,0]\t[0,20000]\t[0,120]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0\n"\
                                   "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t63113904\t0\t0\t0\t0\t0\t0"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t4\t1000\t1\t0\t101\t0\t408\t0\t100\t1000\t400\t0\t0\n"\
                                                "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t63113904\t1\t0\t0\t0\t0\t0\t0\t0\t30000\t0\t0\t0\t20000\t120"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' SET MAX QUERIES = 1000, MAX READ ROWS = 1000 FOR INTERVAL 1 YEAR, "\
                                                  "SET MAX RESULT BYTES = 30000, MAX READ BYTES = 20000, MAX EXECUTION TIME = 120 FOR RANDOMIZED INTERVAL 2 YEAR\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200\n"\
                                   "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t63113904\t1\t0\t50\t200\t50\t200"

    # Remove interval.
    copy_quota_xml('normal_limits.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t1\t0\t50\t200\t50\t200"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t5\t1000\t1\t0\t151\t0\t608\t0\t150\t1000\t600\t0\t0"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\' SET MAX QUERIES = 1000, MAX READ ROWS = 1000 FOR INTERVAL 1 YEAR\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t2\t0\t100\t400\t100\t400"

    # Remove all intervals.
    copy_quota_xml('simpliest.xml')
<<<<<<< HEAD
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
=======
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
    assert instance.query("SHOW CREATE QUOTA") == "CREATE QUOTA myQuota KEYED BY \\'user name\\'\n"
>>>>>>> cfc2904c96... Add DCL to manage quotas.
    
    instance.query("SELECT * from test_table")
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"

    # Add one interval back.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"


def test_add_remove_quota():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"

    # Add quota.
    copy_quota_xml('two_quotas.xml')
<<<<<<< HEAD
    assert system_quotas() ==\
           "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\t[]\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t[]\t0\t[]\t[3600,2629746]\t[1,0]\t[0,0]\t[0,0]\t[4000,0]\t[400000,0]\t[4000,0]\t[400000,0]\t[60,1800]"

    # Drop quota.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
=======
    assert query_select_from_system_quotas() ==\
           "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t\N\t3600\t1\t\N\t0\t\N\t0\t\N\t4000\t\N\t400000\t\N\t4000\t\N\t400000\t60\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t\N\t2629746\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t1800"
    assert instance.query("SHOW CREATE QUOTA myQuota") == "CREATE QUOTA myQuota KEYED BY \\'user name\\'\n"
    assert instance.query("SHOW CREATE QUOTA myQuota2") == "CREATE QUOTA myQuota2 KEYED BY \\'client key or user name\\' SET MAX RESULT ROWS = 4000, MAX RESULT BYTES = 400000, MAX READ ROWS = 4000, MAX READ BYTES = 400000, MAX EXECUTION TIME = 60 FOR RANDOMIZED INTERVAL 1 HOUR, SET MAX EXECUTION TIME = 1800 FOR INTERVAL 1 MONTH\n"

    # Drop inactive quota
    copy_quota_xml('simpliest.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
    assert re.search("Quota.*not\ found", instance.query_and_get_error("SHOW CREATE QUOTA myQuota2"))


def test_users_xml_is_readonly(started_cluster):
    copy_quota_xml('simpliest.xml')
    assert "Cannot update Quota" in instance.query_and_get_error("ALTER QUOTA myQuota SET TRACKING FOR INTERVAL 1 DAY")
    assert "Cannot remove Quota" in instance.query_and_get_error("DROP QUOTA myQuota")
>>>>>>> cfc2904c96... Add DCL to manage quotas.

    # Drop all quotas.
    copy_quota_xml('no_quotas.xml')
    assert system_quotas() == ""
    assert system_quota_usage() == ""

    # Add one quota back.
    copy_quota_xml('normal_limits.xml')
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"
    assert system_quota_usage() == "e651da9c-a748-8703-061a-7e5e5096dae7\tdefault\t31556952\t0\t0\t0\t0\t0\t0"


def test_reload_users_xml_by_timer():
    assert system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1000]\t[0]\t[0]\t[0]\t[1000]\t[0]\t[0]"

    time.sleep(1) # The modification time of the 'quota.xml' file should be different,
                  # because config files are reload by timer only when the modification time is changed.
    copy_quota_xml('tiny_limits.xml', reload_immediately=False)
    assert_eq_with_retry(instance, query_from_system_quotas, "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\t['default']\t0\t[]\t[31556952]\t[0]\t[1]\t[1]\t[1]\t[0]\t[1]\t[0]\t[0]")
