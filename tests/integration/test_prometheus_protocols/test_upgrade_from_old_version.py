import os
import pytest
import shlex

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml", "configs/backups_disk.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
    external_dirs=["/backups/"],
)


# Time series data for "foo" — inserted directly into old inner tables before upgrade.
foo = [({"__name__": "foo", "job": "prometheus"}, {1000.0: 10.0})]

# Time series data for "bar" — inserted via RemoteWrite after upgrade.
bar = [({"__name__": "bar", "job": "prometheus"}, {2000.0: 20.0})]


# DDL for old-schema inner tables (as they existed before this branch).
# Note: `type` and `unit` columns are plain `String`, not `LowCardinality(String)`.
OLD_DATA_DDL = (
    "(id UUID, timestamp DateTime64(3), value Float64)"
    " ENGINE=MergeTree ORDER BY (id, timestamp)"
)
OLD_TAGS_DDL = (
    "(id UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),"
    " metric_name LowCardinality(String),"
    " tags Map(LowCardinality(String), String),"
    " all_tags Map(String, String) EPHEMERAL,"
    " min_time SimpleAggregateFunction(min, Nullable(DateTime64(3))),"
    " max_time SimpleAggregateFunction(max, Nullable(DateTime64(3))))"
    " ENGINE=AggregatingMergeTree ORDER BY (metric_name, id)"
)
OLD_METRICS_DDL = (
    "(metric_family_name String, type String, unit String, help String)"
    " ENGINE=ReplacingMergeTree ORDER BY metric_family_name"
)

# Column list as it would appear in the old metadata file.
OLD_COLUMNS = """\
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `timestamp` DateTime64(3),
    `value` Float64,
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `metric_family_name` String,
    `type` String,
    `unit` String,
    `help` String
)"""

NIL_UUID = "00000000-0000-0000-0000-000000000000"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def cleanup_after_test():
    try:
        yield
    finally:
        node.query("DROP TABLE IF EXISTS default.prometheus SYNC")


def insert_foo_into_inner_tables(data_table, tags_table, metrics_table):
    foo_id = node.query(
        "SELECT reinterpretAsUUID(sipHash128('foo', mapSort(map('__name__', 'foo', 'job', 'prometheus'))))"
    ).strip()
    node.query(f"INSERT INTO `{data_table}` VALUES ('{foo_id}', toDateTime64(1000, 3), 10.0)")
    node.query(
        f"INSERT INTO `{tags_table}` (id, metric_name, tags, all_tags)"
        f" VALUES ('{foo_id}', 'foo', {{'job': 'prometheus'}}, mapSort(map('__name__', 'foo', 'job', 'prometheus')))"
    )
    node.query(f"INSERT INTO `{metrics_table}` VALUES ('foo', 'gauge', 'bytes', 'Foo metric')")


def send_bar_via_remote_write():
    protobuf = convert_time_series_to_protobuf(bar)
    send_protobuf_to_remote_write(node.ip_address, 9093, "/write", protobuf)


def check_foo_and_bar():
    result = node.query(
        "SELECT t.metric_name, d.timestamp, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " JOIN timeSeriesTags(prometheus) AS t ON d.id = t.id"
        " ORDER BY t.metric_name, d.timestamp"
    )
    assert result == TSV([
        ["bar", "1970-01-01 00:33:20.000", "20"],
        ["foo", "1970-01-01 00:16:40.000", "10"],
    ])


# Version 1: Without INNER COLUMNS
def create_and_fill_table_version_1(time_series_columns=OLD_COLUMNS, time_series_settings="",
                                    data_def=OLD_DATA_DDL, tags_def=OLD_TAGS_DDL,
                                    metrics_def=OLD_METRICS_DDL):
    """Creates a TimeSeries table that looks like one created by an old ClickHouse version.

    Creates old-schema inner tables, fills them with `foo` data, then detaches the dummy
    placeholder table and replaces its metadata with a TimeSeries ATTACH statement.
    Works for both Atomic databases (ts_uuid is not nil) and Ordinary databases (ts_uuid is nil).
    """
    node.query("CREATE TABLE prometheus (dummy UInt8) ENGINE=Null")
    settings_clause = f"SETTINGS {time_series_settings}" if time_series_settings else ""

    prometheus_sql_path = node.query(
        "SELECT metadata_path FROM system.tables WHERE database = 'default' AND name = 'prometheus'"
    ).strip()

    ts_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE database = 'default' AND name = 'prometheus'"
    ).strip()

    # Inner table names depend on whether we're in an Atomic database (UUID != nil) or Ordinary.
    if ts_uuid != NIL_UUID:
        data_table_name = f".inner_id.data.{ts_uuid}"
        tags_table_name    = f".inner_id.tags.{ts_uuid}"
        metrics_table_name = f".inner_id.metrics.{ts_uuid}"
    else:
        data_table_name = ".inner.data.prometheus"
        tags_table_name    = ".inner.tags.prometheus"
        metrics_table_name = ".inner.metrics.prometheus"

    node.query(f"CREATE TABLE `{data_table_name}`    {data_def}")
    node.query(f"CREATE TABLE `{tags_table_name}`    {tags_def}")
    node.query(f"CREATE TABLE `{metrics_table_name}` {metrics_def}")

    insert_foo_into_inner_tables(data_table_name, tags_table_name, metrics_table_name)

    data_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{data_table_name}'"
    ).strip()
    tags_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{tags_table_name}'"
    ).strip()
    metrics_uuid = node.query(
        f"SELECT uuid FROM system.tables WHERE database='default' AND name='{metrics_table_name}'"
    ).strip()

    node.query("DETACH TABLE prometheus")

    uuid_clause = f"UUID '{ts_uuid}'" if ts_uuid != NIL_UUID else ""
    inner_uuid_clause = ""
    if data_uuid != NIL_UUID:
        inner_uuid_clause += f"DATA INNER UUID '{data_uuid}'\n"
    if tags_uuid != NIL_UUID:
        inner_uuid_clause += f"TAGS INNER UUID '{tags_uuid}'\n"
    if metrics_uuid != NIL_UUID:
        inner_uuid_clause += f"METRICS INNER UUID '{metrics_uuid}'\n"

    metadata = (
        f"ATTACH TABLE prometheus {uuid_clause}\n"
        f"{time_series_columns}\n"
        f"ENGINE = TimeSeries\n"
        f"{inner_uuid_clause}"
        f"{settings_clause}\n"
    )
    node.exec_in_container(
        ["bash", "-c", f"printf '%s' {shlex.quote(metadata)} > /var/lib/clickhouse/{prometheus_sql_path}"]
    )
    node.query("ATTACH TABLE prometheus")


# Checks that a TimeSeries table created by an old version of ClickHouse (Atomic database)
# can be used by the current version.
def test_upgrade_from_version_1():
    create_and_fill_table_version_1()
    send_bar_via_remote_write()
    check_foo_and_bar()


# Checks that a TimeSeries table created by an old version of ClickHouse (Ordinary database)
# can be used by the current version.
def test_upgrade_from_version_1_ordinary_db():
    node.query("DROP DATABASE default SYNC")
    node.query(
        "CREATE DATABASE default ENGINE=Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_and_fill_table_version_1()
    send_bar_via_remote_write()
    check_foo_and_bar()

    node.query("DROP TABLE default.prometheus SYNC")
    node.query("DROP DATABASE default SYNC")
    node.query("CREATE DATABASE default")


# Checks that a TimeSeries table backed up by an old version of ClickHouse can be restored
# and used by the current version.
def test_restore_from_version_1():
    backup_file = os.path.join(os.path.dirname(__file__), "backups", "time_series_version_1.zip")
    node.copy_file_to_container(backup_file, "/backups/time_series_version_1.zip")
    node.query("RESTORE TABLE default.prometheus FROM Disk('backups', 'time_series_version_1.zip')")
    send_bar_via_remote_write()
    check_foo_and_bar()
