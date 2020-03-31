from threading import Thread
import time

from .cloud import CombinedCloudAPI
from .cloud import init_node
from .cloud import DeploymentStatus, VmStatus
from .cloud import Node, NodeType
from .cloud import PlatformType


class NodeController:
    def __init__(self, manager_settings):
        self.manager_settings = manager_settings
        self.thread = Thread(target=self.control_loop)
        self.api = CombinedCloudAPI(self.manager_settings)

        self.node_types = [NodeType.content]
        self.node_lists = {}
        self.node_count_actual = {}
        self.node_count_target = {}
        self.orphaned_nodes = []

        for t in self.node_types:
            self.node_lists[t] = []
            # sets target node count of each type to 1, so one of each type gets spawned on start
            self.node_count_target[t] = 2
            self.node_count_actual[t] = 0

    def start_loop(self):
        self.thread.start()

    def control_loop(self):
        self.api.init_cloud_apis()
        self.__handle_orphaned_nodes()
        self.__init_static_nodes()

        while True:
            self.__update_node_state()
            if self.manager_settings.cloud_deployment:
                self.__create_nodes_if_necessary()
                self.__init_nodes_if_necessary()
                self.__clean_up_failed_nodes()

            time.sleep(10)

    def __init_static_nodes(self):
        node_type = NodeType.content
        content_nodes_ips = list(map(str.strip, self.manager_settings.static_content_nodes_ips.split(',')))
        for ip in content_nodes_ips:
            new_node = Node("static_content_"+ip, node_type, PlatformType.static, DeploymentStatus.CREATED, ip)
            self.node_lists[node_type].append(new_node)

            if self.manager_settings.init_static_content_nodes:
                try:
                    init_node(new_node)
                except Exception as e:
                    print(e)
                    new_node.deployment_status = DeploymentStatus.INIT_FAILED
            else:
                # expect node to be operational if managed manually.
                new_node.deployment_status = DeploymentStatus.OPERATIONAL

    def __handle_orphaned_nodes(self):
        orphaned_nodes = self.api.get_all_nodes()
        print("Orphaned Nodes:")
        for node in orphaned_nodes:
            print(node)
            if self.manager_settings.remove_orphaned_nodes:
                self.api.delete_node(node)
            else:
                node.node_type = NodeType.orphaned
                self.orphaned_nodes.append(node)

    def __update_node_state(self):
        for t in self.node_types:
            for node in self.node_lists[t]:
                if node.platform == PlatformType.cloud:
                    self.api.update_vm_state(node)

        # TODO: remove debug prints
        print("Currently tracked Nodes:")
        for t in self.node_types:
            print("Type: ", t)
            for n in self.node_lists[t]:
                print(n)

    def __num_good_nodes(self, node_type):
        return sum(map(lambda x: DeploymentStatus.state_ok(x.deployment_status), self.node_lists[node_type]))

    def __init_nodes_if_necessary(self):
        for t in self.node_types:
            for node in self.node_lists[t]:
                if node.deployment_status == DeploymentStatus.CREATED and node.vm_status == VmStatus.RUNNING:
                    try:
                        print("init node: ", node)
                        # time.sleep(30)
                        init_node(node)
                    except Exception as e:
                        print(e)
                        node.deployment_status = DeploymentStatus.INIT_FAILED

    def __create_nodes_if_necessary(self):
        for t in self.node_types:
            if self.__num_good_nodes(t) < self.node_count_target[t]:
                # TODO sanity check, max number of nodes
                new_node = None
                try:
                    new_node = self.api.create_node(node_type=t)
                    self.node_lists[t].append(new_node)
                except Exception as e:
                    print(e)
                    new_node.deployment_status = DeploymentStatus.CREATION_FAILED

    def __clean_up_failed_nodes(self):
        for t in self.node_types:
            for node in self.node_lists[t]:
                if node.platform == PlatformType.cloud and not DeploymentStatus.state_ok(node.deployment_status):
                    self.api.delete_node(node)
                    self.node_lists[t].remove(node)
