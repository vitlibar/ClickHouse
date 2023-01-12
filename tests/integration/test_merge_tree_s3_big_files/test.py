import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/storage_conf.xml",
        "configs/disable_logs.xml",
    ],
    with_minio=True,
    env_variables={"ASAN_OPTIONS": "use_sigaltstack=false", "TSAN_OPTIONS": "use_sigaltstack=false", "MSAN_OPTIONS": "use_sigaltstack=false"},
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_upload_big_files_s3():
    node.query("DROP TABLE IF EXISTS test")
    node.query(
        "CREATE TABLE test(col0 UInt8 CODEC (NONE), col1 FixedString(100) CODEC (NONE)) ENGINE = MergeTree ORDER BY tuple()"
        "SETTINGS storage_policy = 's3', index_granularity=1048576, index_granularity_bytes=104857600"
    )

    # Insert 100 millions rows.
    node.query(
        "INSERT INTO test(col0) SELECT 0 from zeros(100000000) settings max_block_size=40000000"
    )

    # Produce a single big part.
    node.query("OPTIMIZE TABLE test FINAL")

    size = int(node.query(
        "SELECT bytes_on_disk FROM system.parts WHERE database = 'default' AND table='test' AND active"
    ).strip())

    assert size < 0
