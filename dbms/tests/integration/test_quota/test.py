import pytest
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
import os
import re
import time

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance('instance',
                                config_dir="configs")


columns_without_execution_time = "name, id, source, key_type, key, duration, randomize_interval, "\
                                 "queries, max_queries, errors, max_errors, result_rows, max_result_rows, result_bytes, max_result_bytes, "\
                                 "read_rows, max_read_rows, read_bytes, max_read_bytes, max_execution_time"

select_from_system_quotas = "SELECT " + columns_without_execution_time + " FROM system.quotas ORDER BY name, key, duration";

def query_select_from_system_quotas():
    return instance.query(select_from_system_quotas).rstrip('\n')


def copy_quota_xml(local_file_name, reload_immediately = True):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    instance.copy_file_to_container(os.path.join(script_dir, local_file_name), '/etc/clickhouse-server/users.d/quota.xml')
    if reload_immediately:
       instance.query("SYSTEM RELOAD CONFIG")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        
        instance.query("CREATE TABLE test_table(x UInt32) ENGINE = MergeTree ORDER BY tuple()")
        instance.query("INSERT INTO test_table SELECT number FROM numbers(50)")

        yield cluster

    finally:
        cluster.shutdown()


def test_quota_from_users_xml(started_cluster):
    assert instance.query("SELECT currentQuota()") == "myQuota\n"
    assert instance.query("SELECT currentQuotaID()") == "e651da9c-a748-8703-061a-7e5e5096dae7\n"
    assert instance.query("SELECT currentQuotaKey()") == "default\n"

    # Simpliest doesn't even track usage.
    copy_quota_xml('simpliest.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N"

    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N"

    # Change quota, now we're tracking usage.
    copy_quota_xml('tracking.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0"

    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t1\t0\t0\t0\t50\t0\t200\t0\t50\t0\t200\t0\t0"

    instance.query("SELECT COUNT() from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t2\t0\t0\t0\t51\t0\t208\t0\t50\t0\t200\t0\t0"

    # Change quota, now the limits are tiny so we will exceed the quota.
    copy_quota_xml('tiny_limits.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t2\t1\t0\t1\t51\t1\t208\t0\t50\t1\t200\t0\t0"

    assert re.search("Quota.*has\ been\ exceeded", instance.query_and_get_error("SELECT * from test_table"))
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t3\t1\t1\t1\t51\t1\t208\t0\t50\t1\t200\t0\t0"

    # Change quota, now the limits are enough to execute queries.
    copy_quota_xml('normal_limits.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t3\t1000\t1\t0\t51\t0\t208\t0\t50\t1000\t200\t0\t0"
    
    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t4\t1000\t1\t0\t101\t0\t408\t0\t100\t1000\t400\t0\t0"

    # Add interval.
    copy_quota_xml('two_intervals.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t4\t1000\t1\t0\t101\t0\t408\t0\t100\t1000\t400\t0\t0\n"\
                                                "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t63113904\t1\t0\t0\t0\t0\t0\t0\t0\t30000\t0\t0\t0\t20000\t120"
    
    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t5\t1000\t1\t0\t151\t0\t608\t0\t150\t1000\t600\t0\t0\n"\
                                                "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t63113904\t1\t1\t0\t0\t0\t50\t0\t200\t30000\t50\t0\t200\t20000\t120"

    # Remove interval.
    copy_quota_xml('normal_limits.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t5\t1000\t1\t0\t151\t0\t608\t0\t150\t1000\t600\t0\t0"
    
    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t6\t1000\t1\t0\t201\t0\t808\t0\t200\t1000\t800\t0\t0"

    # Remove all intervals.
    copy_quota_xml('simpliest.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"
    
    instance.query("SELECT * from test_table")
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"

    # Add inactive quota.
    copy_quota_xml('two_quotas.xml')
    assert query_select_from_system_quotas() ==\
           "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t\N\t3600\t1\t\N\t0\t\N\t0\t\N\t4000\t\N\t400000\t\N\t4000\t\N\t400000\t60\n"\
           "myQuota2\t4590510c-4d13-bf21-ec8a-c2187b092e73\tusers.xml\tclient key or user name\t\N\t2629746\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t\N\t0\t1800"

    # Drop inactive quota
    copy_quota_xml('simpliest.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t\\N"


def test_reload_users_xml_by_timer(started_cluster):
    copy_quota_xml('simpliest.xml')
    assert query_select_from_system_quotas() == "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N\t\N"

    time.sleep(1) # The modification time of the 'quota.xml' file should be different,
                  # because config files are reload by timer only when the modification time is changed.
    copy_quota_xml('tracking.xml', reload_immediately=False)
    assert_eq_with_retry(instance, select_from_system_quotas, "myQuota\te651da9c-a748-8703-061a-7e5e5096dae7\tusers.xml\tuser name\tdefault\t31556952\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0")
