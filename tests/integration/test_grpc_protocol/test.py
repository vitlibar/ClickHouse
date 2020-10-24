import os
import pytest
import subprocess
import sys
import grpc
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Use grpcio-tools to generate *pb2.py files from *.proto.

proto_dir = os.path.join(SCRIPT_DIR, './protos')
proto_gen_dir = os.path.join(SCRIPT_DIR, './_gen')
os.makedirs(proto_gen_dir, exist_ok=True)
subprocess.check_call(
    'python3 -m grpc_tools.protoc -I{proto_dir} --python_out={proto_gen_dir} --grpc_python_out={proto_gen_dir} \
    {proto_dir}/clickhouse_grpc.proto'.format(proto_dir=proto_dir, proto_gen_dir=proto_gen_dir), shell=True)

sys.path.append(proto_gen_dir)
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc


# Utilities

config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', main_configs=['configs/grpc_port.xml'])
grpc_port = 9001
main_stub = None

def create_channel():
    node_ip_with_grpc_port = cluster.get_instance_ip('node') + ':' + str(grpc_port)
    return grpc.insecure_channel(node_ip_with_grpc_port)

def create_stub(channel):
    grpc.channel_ready_future(channel).result()
    return clickhouse_grpc_pb2_grpc.ClickHouseStub(channel)

def query_common(query_text, input_data=[], format='TabSeparated', settings={}, session_id="", stub=None):
    if type(input_data) == str:
        input_data = [input_data]
    if not stub:
        stub = main_stub
    def send_query_info():
        input_data_part = input_data.pop(0) if input_data else ''
        yield clickhouse_grpc_pb2.QueryInfo(user_name='default', quota='default', query=query_text, query_id='123', format=format,
                                            input_data=input_data_part, use_next_input_data = bool(input_data), settings=settings,
                                            session_id=session_id)
        while input_data:
            input_data_part = input_data.pop(0)
            yield clickhouse_grpc_pb2.QueryInfo(input_data=input_data_part, use_next_input_data=bool(input_data))
    return list(stub.ExecuteQuery(send_query_info()))

def query_no_errors(*args, **kwargs):
    results = query_common(*args, **kwargs)
    if results and results[-1].HasField('exception'):
        raise Exception(results[-1].exception.display_text)
    return results

def query(*args, **kwargs):
    output = ""
    for result in query_no_errors(*args, **kwargs):
        output += result.output
    return output

def query_and_get_error(*args, **kwargs):
    results = query_common(*args, **kwargs)
    if not results or not results[-1].HasField('exception'):
        raise Exception("Expected to be failed but succeeded!")
    return results[-1].exception

def query_and_get_totals(*args, **kwargs):
    totals = ""
    for result in query_no_errors(*args, **kwargs):
        totals += result.totals
    return totals

def query_and_get_extremes(*args, **kwargs):
    extremes = ""
    for result in query_no_errors(*args, **kwargs):
        extremes += result.extremes
    return extremes

def query_and_get_logs(*args, **kwargs):
    logs = ""
    for result in query_no_errors(*args, **kwargs):
        for log_entry in result.logs:
            #print(log_entry)
            logs += log_entry.text + "\n"
    return logs

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        with create_channel() as channel:
            global main_stub
            main_stub = create_stub(channel)
            yield cluster
            
    finally:
        cluster.shutdown()

@pytest.fixture(autouse=True)
def reset_after_test():
    yield
    query("DROP TABLE IF EXISTS t")

# Actual tests

def test_select_one():
    assert query("SELECT 1") == "1\n"

def test_ordinary_query():
    assert query("SELECT count() FROM numbers(100)") == "100\n"

def test_insert_query():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1),(2),(3)")
    query("INSERT INTO t FORMAT TabSeparated 4\n5\n6\n")
    query("INSERT INTO t VALUES", input_data="(7),(8)")
    query("INSERT INTO t FORMAT TabSeparated", input_data="9\n10\n")
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n"

def test_insert_query_streaming():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES", input_data=["(1),(2),(3)", "(5),(4),(6)", "(8),(7),(9)"])
    assert query("SELECT a FROM t ORDER BY a") == "1\n2\n3\n4\n5\n6\n7\n8\n9\n"

def test_output_format():
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1),(2),(3)")
    assert query("SELECT a FROM t ORDER BY a FORMAT JSONEachRow") == '{"a":1}\n{"a":2}\n{"a":3}\n'
    assert query("SELECT a FROM t ORDER BY a", format="JSONEachRow") == '{"a":1}\n{"a":2}\n{"a":3}\n'

def test_totals_and_extremes():
    query("CREATE TABLE t (x UInt8, y UInt8) ENGINE = Memory")
    query("INSERT INTO t VALUES (1, 2), (2, 4), (3, 2), (3, 3), (3, 4)")
    assert query("SELECT sum(x), y FROM t GROUP BY y WITH TOTALS") == "4\t2\n3\t3\n5\t4\n"
    assert query_and_get_totals("SELECT sum(x), y FROM t GROUP BY y WITH TOTALS") == "12\t0\n"
    assert query("SELECT x, y FROM t") == "1\t2\n2\t4\n3\t2\n3\t3\n3\t4\n"
    assert query_and_get_extremes("SELECT x, y FROM t", settings={"extremes": "1"}) == "1\t2\n3\t4\n"

def test_errors_handling():
    e = query_and_get_error("")
    #print(e)
    assert "Empty query" in e.display_text
    query("CREATE TABLE t (a UInt8) ENGINE = Memory")
    e = query_and_get_error("CREATE TABLE t (a UInt8) ENGINE = Memory")
    assert "Table default.t already exists" in e.display_text

def test_logs():
    logs = query_and_get_logs("SELECT 1", settings={'send_logs_level':'debug'})
    assert "SELECT 1" in logs
    assert "Read 1 rows" in logs
    assert "Peak memory usage" in logs

def test_progress():
    results = query_no_errors("select number, sleep(0.320) from numbers(8) settings max_block_size=2")
    assert str(results) ==\
"""[progress {
  read_rows: 2
  read_bytes: 16
  total_rows_to_read: 8
}
, output: "0\\t0\\n1\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "2\\t0\\n3\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "4\\t0\\n5\\t0\\n"
, progress {
  read_rows: 2
  read_bytes: 16
}
, output: "6\\t0\\n7\\t0\\n"
, ]"""

def test_session():
    session_a = "session A"
    session_b = "session B"
    query("SET custom_x=1", session_id=session_a)
    query("SET custom_y=2", session_id=session_a)
    query("SET custom_x=3", session_id=session_b)
    query("SET custom_y=4", session_id=session_b)
    assert query("SELECT getSetting('custom_x'), getSetting('custom_y')", session_id=session_a) == "1\t2\n"
    assert query("SELECT getSetting('custom_x'), getSetting('custom_y')", session_id=session_b) == "3\t4\n"

def test_no_session():
    e = query_and_get_error("SET custom_x=1")
    assert "There is no session" in e.display_text
