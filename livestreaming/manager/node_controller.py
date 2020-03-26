from threading import Thread
import time

from .cloud import CombinedCloudAPI


class NodeController:
    def __init__(self, manager_settings):
        self.manager_settings = manager_settings
        self.thread = Thread(target=self.control_loop)
        self.api = CombinedCloudAPI(self.manager_settings)

        self.target_node_count = 3
        self.actual_node_count = -1

    def start_loop(self):
        self.thread.start()

    def control_loop(self):
        self.api.init_cloud_apis()
        while True:
            # print('Tick...')
            # TODO implement control loop
            # TODO check current state against desired state of cloud deployment
            # TODO spawn new nodes or destroy nodes if necessary
            # self.__get_current_cloud_state()
            # self.__create_nodes_if_necessary()
            self.api.pick_node_name()

            time.sleep(3)

    def __get_current_cloud_state(self):
        nodes = self.api.get_all_nodes()
        print("*** Nodes ***")
        for n in nodes:
            print(n)
        print("")
        self.actual_node_count = len(nodes)

    def __create_nodes_if_necessary(self):
        if self.actual_node_count < self.target_node_count:
            pass
