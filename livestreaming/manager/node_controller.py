from threading import Thread
import time

from .cloud import CombinedCloudAPI
from .cloud import init_node
from .cloud import DeploymentStatus
from .cloud import Node
from .cloud import PlatformType


class NodeController:
    def __init__(self, manager_settings):
        self.manager_settings = manager_settings
        self.thread = Thread(target=self.control_loop)
        self.api = CombinedCloudAPI(self.manager_settings)

        self.node_types = ['content']
        self.node_lists = {}
        self.node_count_actual = {}
        self.node_count_target = {}

        for t in self.node_types:
            self.node_lists[t] = []
            # sets target node count of each type to 1, so one of each type gets spawned on start
            self.node_count_target[t] = 1
            self.node_count_actual[t] = 0

    def start_loop(self):
        self.thread.start()

    def control_loop(self):
        self.api.init_cloud_apis()
        self.__init_static_nodes()

        while True:
            # print('Tick...')
            # TODO implement control loop
            # TODO check current state against desired state of cloud deployment
            # TODO spawn new nodes or destroy nodes if necessary
            self.__get_current_node_state()
            if self.manager_settings.cloud_deployment:
                self.__create_nodes_if_necessary()

            time.sleep(10)

    def __init_static_nodes(self):
        # TODO initialise static nodes if configured
        if self.manager_settings.init_static_content_nodes:
            node_type = "content"
            content_nodes_ips = list(map(str.strip, self.manager_settings.static_content_nodes_ips.split(',')))
            for ip in content_nodes_ips:
                new_node = Node("static_content_"+ip, node_type, PlatformType.static, DeploymentStatus.CREATED, ip)
                self.node_lists[node_type].append(new_node)
                init_node(new_node)

    def __get_current_node_state(self):
        print("Currently tracked Nodes:")
        for t in self.node_types:
            for n in self.node_lists[t]:
                print(n)

    def __num_operational_nodes(self, node_type):
        return sum(map(lambda x: x.deployment_status == DeploymentStatus.OPERATIONAL, self.node_lists[node_type]))

    def __create_nodes_if_necessary(self):
        for t in self.node_types:
            if self.__num_operational_nodes(t) < self.node_count_target[t]:
                # spawn a node of type t
                try:
                    # TODO sanity check, max number of nodes
                    new_node = self.api.create_node(node_type=t)
                    self.node_lists[t].append(new_node)
                    time.sleep(30)
                    # TODO mechanism for checking if node is up
                    init_node(new_node)
                except Exception as e:
                    print(e)
                    # TODO: handle node creation exception
