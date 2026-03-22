import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import tsv_close_to


cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    user_configs=["configs/allow_experimental_time_series_table.xml"],
    clickhouse_path_dir="old_version_clickhouse_path"
)


timestamp = 1753199684.626

# Queries to check.
test_queries = [
    (
        'up{instance=~"demo-service-0:.*"}',
        '{"resultType": "vector", "result": [{"metric": {"__name__": "up", "instance": "demo-service-0:10000", "job": "demo"}, "value": [1753199684.626, "1"]}]}',
        [
            [
                "[('__name__','up'),('instance','demo-service-0:10000'),('job','demo')]",
                "2025-07-22 15:54:44.626",
                "1",
            ]
        ],
    ),
    (
        'irate(prometheus_http_requests_total{code="200",handler="/api/v1/query"}[30s])',
        '{"resultType": "vector", "result": [{"metric": {"code": "200", "handler": "/api/v1/query", "instance": "prometheus:9090", "job": "prometheus"}, "value": [1753199684.626, "0.2"]}]}',
        [
            [
                "[('code','200'),('handler','/api/v1/query'),('instance','prometheus:9090'),('job','prometheus')]",
                "2025-07-22 15:54:44.626",
                "0.2",
            ]
        ],
    ),
]


# Executes the test queries in ClickHouse and test the results.
def check_queries_in_clickhouse(table_name):
    for query, _, chresult in test_queries:
        assert tsv_close_to(
            node.query(f"SELECT * FROM prometheusQuery({table_name}, '{query}', {timestamp})"),
            chresult,
            eps=1e-9,
        )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


# Test that we can attach a TimeSeries table created by the first version.
def test_attach_old_version():
    check_queries_in_clickhouse("ts01")


# Test that we can restore a TimeSeries table created by the first version.
#def test_restore_old_version():
#    pass
