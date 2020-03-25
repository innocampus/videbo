from threading import Thread
import time

from .cloud import CombinedCloudAPI


class NodeController:
    def __init__(self, manager_settings):
        self.manager_settings = manager_settings
        self.thread = Thread(target=self.control_loop)
        self.api = CombinedCloudAPI(self.manager_settings)

    def start_loop(self):
        self.thread.start()

    def control_loop(self):
        self.api.init_cloud_apis()
        while True:
            # print('Tick...')
            # TODO implement control loop
            # TODO check current state against desired state of cloud deployment
            # TODO spawn new nodes or destroy nodes if necessary
            time.sleep(3)
