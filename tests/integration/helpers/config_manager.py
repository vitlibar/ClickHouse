import os


class ConfigManager:
    """ Allows to temporarily add a configuration file to the "config.d" or "users.d" directories in a docker container.

    Usage:
    with ConfigManager() as cm:
        cm.add_main_config("configs/test_specific_config.xml") # copy "configs/test_specific_config.xml" to "/etc/clickhouse-server/config.d"
        ...
        # "/etc/clickhouse-server/config.d/test_specific_config.xml" is removed automatically

    """

    def __init__(self):
        self.__added_configs = []

    def add_main_config(self, node_or_nodes, local_path, reload_config=True):
        self.__add_config(node_or_nodes, local_path, dest_dir='config.d', reload_config=reload_config)

    def add_user_config(self, node_or_nodes, local_path, reload_config=True):
        self.__add_config(node_or_nodes, local_path, dest_dir='users.d', reload_config=reload_config)

    def __add_config(self, node_or_nodes, local_path, dest_dir, reload_config):
        nodes_to_add_config = node_or_nodes if (type(node_or_nodes) is list) else [node_or_nodes]
        for node in nodes_to_add_config:
            src_path = os.path.join(node.cluster.base_dir, local_path)
            dest_path = os.path.join("/etc/clickhouse-server", dest_dir, os.path.basename(local_path))
            node.copy_file_to_container(src_path, dest_path)
        if reload_config:
            for node in nodes_to_add_config:
                node.query("SYSTEM RELOAD CONFIG")
        for node in nodes_to_add_config:
            dest_path = os.path.join("/etc/clickhouse-server", dest_dir, os.path.basename(local_path))
            self.__added_configs.append((node, dest_path))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for node, dest_path in self.__added_configs:
            node.remove_file_from_container(dest_path)
        for node, _ in self.__added_configs:
            node.query("SYSTEM RELOAD CONFIG")
