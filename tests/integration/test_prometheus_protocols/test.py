import pytest
import time
import requests
from http import HTTPStatus
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=["configs/prometheus.xml"],
    with_prometheus=True,
    handle_prometheus_remote_write=True,
    handle_prometheus_remote_read=True,
)


def execute_query_on_prometheus_writer(query, timestamp):
    return execute_query_impl(
        cluster.get_instance_ip(cluster.prometheus_writer_host),
        cluster.prometheus_writer_port,
        "/api/v1/query",
        query,
        timestamp,
    )


def execute_query_on_prometheus_reader(query, timestamp):
    return execute_query_impl(
        cluster.get_instance_ip(cluster.prometheus_reader_host),
        cluster.prometheus_reader_port,
        "/api/v1/query",
        query,
        timestamp,
    )


def execute_query_impl(host, port, path, query, timestamp):
    if not path.startswith("/"):
        path += "/"
    url = f"http://{host}:{port}/{path.strip('/')}?query={query}&time={timestamp}"
    print(f"Requesting {url}")
    r = requests.get(url)
    print(f"Status code: {r.status_code} {HTTPStatus(r.status_code).phrase}")
    if r.status_code != requests.codes.ok:
        print(f"Response: {r.text}")
        raise Exception(f"Got unexpected status code {r.status_code}")
    return r.json()


def show_query_result(query):
    evaluation_time = time.time()
    print(f"Evaluating query: {query}")
    print(f"Evaluation time: {evaluation_time}")
    result_from_writer = execute_query_on_prometheus_writer(query, evaluation_time)
    print(f"Result from prometheus_writer: {result_from_writer}")
    result_from_reader = execute_query_on_prometheus_reader(query, evaluation_time)
    print(f"Result from prometheus_reader: {result_from_reader}")


def compare_query_results(query):
    timeout = 30
    start_time = time.time()
    evaluation_time = start_time
    print(f"Evaluating query: {query}")
    print(f"Evaluation time: {evaluation_time}")
    while time.time() < start_time + timeout:
        result_from_writer = execute_query_on_prometheus_writer(query, evaluation_time)
        result_from_reader = execute_query_on_prometheus_reader(query, evaluation_time)
        print(f"Result from prometheus_writer: {result_from_writer}")
        print(f"Result from prometheus_reader: {result_from_reader}")
        if result_from_writer == result_from_reader:
            return
        time.sleep(1)
    raise Exception(
        f"Got different results from prometheus_writer and prometheus_reader"
    )


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_simplest_query():
    for step in range(0, 10):
        time.sleep(step != 0)
        compare_query_results("up")
