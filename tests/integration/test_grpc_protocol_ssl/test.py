import os
import pytest
import subprocess
import sys
import grpc
from helpers.cluster import ClickHouseCluster

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


# Use grpcio-tools to generate *pb2.py files from *.proto.

proto_dir = os.path.join(SCRIPT_DIR, './protos')
gen_dir = os.path.join(SCRIPT_DIR, './_gen')
os.makedirs(gen_dir, exist_ok=True)
subprocess.check_call(
    'python3 -m grpc_tools.protoc -I{proto_dir} --python_out={gen_dir} --grpc_python_out={gen_dir} \
    {proto_dir}/clickhouse_grpc.proto'.format(proto_dir=proto_dir, gen_dir=gen_dir), shell=True)

sys.path.append(gen_dir)
import clickhouse_grpc_pb2
import clickhouse_grpc_pb2_grpc


# Utilities

node_ip = '10.5.172.77' # It's important for the node to work at this IP because 'server-cert.pem' requires that (see server-ext.cnf).
grpc_port = 9001
config_dir = os.path.join(SCRIPT_DIR, './configs')
cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', ipv4_address=node_ip, main_configs=['configs/grpc_config.xml', 'configs/server-key.pem', 'configs/server-cert.pem', 'configs/ca-cert.pem'])
main_channel = None

def create_channel():
    node_ip_with_grpc_port = node_ip + ':' + str(grpc_port)
    ca_cert = open(os.path.join(config_dir, 'ca-cert.pem'), 'rb').read()
    client_key = open(os.path.join(config_dir, 'client-key.pem'), 'rb').read()
    client_cert = open(os.path.join(config_dir, 'client-cert.pem'), 'rb').read()
    credentials = grpc.ssl_channel_credentials(ca_cert, client_key, client_cert)
    channel = grpc.secure_channel(node_ip_with_grpc_port, credentials)
    grpc.channel_ready_future(channel).result(timeout=10)
    global main_channel
    if not main_channel:
        main_channel = channel
    return channel

def query(query_text):
    query_info = clickhouse_grpc_pb2.QueryInfo(query=query_text)
    stub = clickhouse_grpc_pb2_grpc.ClickHouseStub(main_channel)
    result = stub.ExecuteQuery(query_info)
    if result and result.HasField('exception'):
        raise Exception(result.exception.display_text)
    return result.output

@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    cluster.start()
    try:
        with create_channel() as channel:
            yield cluster
            
    finally:
        cluster.shutdown()

# Actual tests

def test_ok():
    assert query("SELECT 'ok'") == "ok\n"
