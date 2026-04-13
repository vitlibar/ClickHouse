import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV
from .prometheus_test_utils import *


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    handle_prometheus_remote_write=(9093, "/write"),
    handle_prometheus_remote_read=(9093, "/read"),
)


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


def test_insert_basic():
    """Test basic INSERT into TimeSeries table and verify data in inner tables."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu_usage', {'job': 'test', 'instance': 'localhost:9090'}, [(toDateTime64(1000, 3), 0.5), (toDateTime64(2000, 3), 0.7)])"
    )

    # Check samples table.
    result = node.query(
        "SELECT d.timestamp, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " ORDER BY d.timestamp"
    )
    assert result == TSV([
        ["1970-01-01 00:16:40.000", "0.5"],
        ["1970-01-01 00:33:20.000", "0.7"],
    ])

    # Check tags table.
    result = node.query(
        "SELECT t.metric_name, t.tags"
        " FROM timeSeriesTags(prometheus) AS t"
    )
    assert result == TSV([["cpu_usage", "{'instance':'localhost:9090','job':'test'}"]])


def test_insert_multiple_rows():
    """Test INSERT with multiple rows."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('metric_a', {'job': 'a'}, [(toDateTime64(1000, 3), 1.0)]),"
        " ('metric_b', {'job': 'b'}, [(toDateTime64(2000, 3), 2.0)])"
    )

    result = node.query(
        "SELECT t.metric_name, d.value"
        " FROM timeSeriesData(prometheus) AS d"
        " JOIN timeSeriesTags(prometheus) AS t ON d.id = t.id"
        " ORDER BY t.metric_name"
    )
    assert result == TSV([
        ["metric_a", "1"],
        ["metric_b", "2"],
    ])


def test_insert_with_metrics_metadata():
    """Test INSERT with metric_family, type, unit, help columns."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series, metric_family, type, unit, help) VALUES"
        " ('http_requests', {'method': 'GET'}, [(toDateTime64(1000, 3), 100.0)], 'http_requests', 'counter', 'requests', 'Total HTTP requests')"
    )

    # Check metrics table.
    result = node.query(
        "SELECT metric_family_name, type, unit, help"
        " FROM timeSeriesMetrics(prometheus)"
    )
    assert result == TSV([["http_requests", "counter", "requests", "Total HTTP requests"]])

    # Check samples.
    result = node.query(
        "SELECT d.value FROM timeSeriesData(prometheus) AS d"
    )
    assert result == TSV([["100"]])


def test_insert_empty_metric_family_skipped():
    """Test that rows with empty metric_family don't produce metrics table entries."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('test_metric', {'job': 'test'}, [(toDateTime64(1000, 3), 42.0)])"
    )

    # Metrics table should be empty (no metric_family provided).
    result = node.query(
        "SELECT count() FROM timeSeriesMetrics(prometheus)"
    )
    assert result == TSV([["0"]])

    # But samples and tags should have data.
    result = node.query(
        "SELECT count() FROM timeSeriesData(prometheus)"
    )
    assert result == TSV([["1"]])


def test_insert_with_tags_to_columns():
    """Test INSERT with tags_to_columns setting."""
    node.query(
        "CREATE TABLE prometheus (job String) ENGINE=TimeSeries"
        " SETTINGS tags_to_columns = {'job': 'job'}"
    )

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('cpu', {'job': 'node_exporter', 'instance': 'host1'}, [(toDateTime64(1000, 3), 0.9)])"
    )

    # Check that 'job' tag went to the dedicated column, not the tags map.
    result = node.query(
        "SELECT t.metric_name, t.job, t.tags"
        " FROM timeSeriesTags(prometheus) AS t"
    )
    assert result == TSV([["cpu", "node_exporter", "{'instance':'host1'}"]])


def test_insert_empty_time_series_skipped():
    """Test that rows with empty time_series arrays are skipped."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('empty_metric', {'job': 'test'}, []),"
        " ('real_metric', {'job': 'test'}, [(toDateTime64(1000, 3), 1.0)])"
    )

    # Only the non-empty row should be inserted.
    result = node.query(
        "SELECT count() FROM timeSeriesData(prometheus)"
    )
    assert result == TSV([["1"]])

    result = node.query(
        "SELECT count() FROM timeSeriesTags(prometheus)"
    )
    assert result == TSV([["1"]])


def test_insert_and_query_with_prometheus_query():
    """Test INSERT and then query via prometheusQuery() function."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('up', {'job': 'prometheus', 'instance': 'localhost:9090'}, [(toDateTime64(1710000000, 3), 1.0)])"
    )

    result = node.query(
        "SELECT * FROM prometheusQuery(prometheus, 'up', 1710000000)"
    )
    # Result should contain the 'up' metric with labels.
    assert "up" in result
    assert "1" in result


def test_insert_metric_name_in_tags():
    """Test that metric_name can be provided inside the tags map as __name__."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (tags, time_series) VALUES"
        " ({'__name__': 'from_tags', 'job': 'test'}, [(toDateTime64(1000, 3), 99.0)])"
    )

    result = node.query(
        "SELECT t.metric_name FROM timeSeriesTags(prometheus) AS t"
    )
    assert result == TSV([["from_tags"]])


def test_insert_consistent_ids():
    """Test that IDs in tags and samples tables are consistent."""
    node.query("CREATE TABLE prometheus ENGINE=TimeSeries")

    node.query(
        "INSERT INTO prometheus (metric_name, tags, time_series) VALUES"
        " ('test', {'k': 'v'}, [(toDateTime64(1000, 3), 1.0), (toDateTime64(2000, 3), 2.0)])"
    )

    # All samples should have the same ID as the corresponding tag row.
    result = node.query(
        "SELECT count(DISTINCT d.id) FROM timeSeriesData(prometheus) AS d"
    )
    assert result == TSV([["1"]])

    result = node.query(
        "SELECT count() FROM timeSeriesData(prometheus) AS d"
        " JOIN timeSeriesTags(prometheus) AS t ON d.id = t.id"
    )
    assert result == TSV([["2"]])
