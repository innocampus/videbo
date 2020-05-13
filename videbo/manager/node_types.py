from asyncio import Task, sleep, Lock, gather, Event
from typing import Optional, Dict, List
from videbo.web import HTTPClient, HTTPResponseError
from videbo.storage.api.models import StorageStatus, DistributorNodeInfo
from videbo.distributor.api.models import DistributorStatus, DistributorCopyFileStatus
from .cloud.server import Server
from . import logger


class NodeTypeBase:
    def __init__(self):
        self.lifecycle_task: Optional[Task] = None
        self.server: Optional[Server] = None
        self.base_url: Optional[str] = None  # does not end with a slash
        self.id: Optional[int] = None  # internal id of the node
        self.created_manually: bool = False  # node was created explicitly by the admin but it is not a static node
        self.shutdown: bool = False  # shut down when watchdog is cancelled
        self.enabled: bool = True  # this node is about to shut down

    async def watchdog(self):
        raise NotImplementedError()

    def is_operational(self) -> bool:
        return self.server is not None and self.server.is_operational()


class BrokerNode(NodeTypeBase):
    async def watchdog(self): # TODO
        while True:
            await sleep(1)


class StorageNode(NodeTypeBase):
    def __init__(self):
        super().__init__()
        self.tx_current_rate: int = 0  # in Mbit/s
        self.tx_max_rate: int = 0  # in Mbit/s
        self.rx_current_rate: int = 0  # in Mbit/s
        self.tx_total: int = 0  # in MB
        self.rx_total: int = 0  # in MB
        self.current_connections: Optional[int] = 0  # HTTP connections serving videos
        self.files_total_size: int = 0  # in MB
        self.files_count: int = 0
        self.free_space: int = 0  # in MB

        self.tx_load: float = 0.0  # tx_current_rate / tx_max_rate
        self.dist_nodes: Dict[str, "DistributorNode"] = {}  # maps base_url to node, nodes that the storage should use

        # since when is the tx load too high, value managed by the StorageDistributorController
        self.load_threshold_exceeded_since: Optional[float] = None

    async def watchdog(self):
        status_url = f"{self.base_url}/api/storage/status"
        first_request = True
        while True:
            try:
                ret: StorageStatus
                status, ret = await HTTPClient.internal_request_manager('GET', status_url, None, StorageStatus)
                if status == 200:
                    self.tx_current_rate = ret.tx_current_rate
                    self.tx_max_rate = ret.tx_max_rate
                    self.rx_current_rate = ret.rx_current_rate
                    self.tx_total = ret.tx_total
                    self.rx_total = ret.rx_total
                    self.current_connections = ret.current_connections
                    self.files_total_size = ret.files_total_size
                    self.files_count = ret.files_count
                    self.free_space = ret.free_space
                    self.tx_load = self.tx_current_rate / self.tx_max_rate

                    # Check that storage should really use all these nodes.
                    for dist_node_base_url in ret.distributor_nodes:
                        if dist_node_base_url not in self.dist_nodes:
                            await self._remove_dist_node(dist_node_base_url)

                    # Check that storage knows about all dist nodes.
                    # Copy to list as we might modify the dict in another coroutine.
                    for dist_base_url, dist_node in list(self.dist_nodes.items()):
                        if dist_base_url not in ret.distributor_nodes:
                            try:
                                await self._add_dist_node(dist_node)
                            except AddDistributorError:
                                pass

                    if first_request:
                        logger.info(f"<Storage watcher {self.server.name}> free_space={self.free_space} MB")
                        first_request = False

                else:
                    logger.error(f"<Storage watcher {self.server.name}> error: http status {status}")

            except HTTPResponseError:
                logger.exception(f"<Storage watcher {self.server.name}> http error")
            await sleep(6)

    async def _add_dist_node(self, node: "DistributorNode"):
        try:
            url = f"{self.base_url}/api/storage/distributor/add"
            info = DistributorNodeInfo(base_url=node.base_url)
            status, ret = await HTTPClient.internal_request_manager('POST', url, info)
            if status == 200:
                logger.info(f"Storage: add distributor node {node.server.name} to storage on {self.server.name}")
            else:
                logger.error(f"Storage: Error adding distributor node {node.server.name} to storage on "
                             f"{self.server.name}: got http status {status}")
                raise AddDistributorError(self)

        except HTTPResponseError as error:
            logger.exception(f"Storage: Error adding distributor node {node.server.name} to storage on "
                             f"{self.server.name}, http error")
            raise AddDistributorError(self)

    def add_dist_node(self, node: "DistributorNode"):
        self.dist_nodes[node.base_url] = node

    def remove_dist_node(self, node_base_url: str):
        self.dist_nodes.pop(node_base_url, None)

    async def _remove_dist_node(self, node_base_url: str):
        try:
            url = f"{self.base_url}/api/storage/distributor/remove"
            info = DistributorNodeInfo(base_url=node_base_url)
            status, ret = await HTTPClient.internal_request_manager('POST', url, info)
            if status == 200:
                logger.info(f"Storage: removed distributor node {node_base_url} on storage on {self.server.name}")
            else:
                logger.error(f"Storage: could not remove distributor node {node_base_url} on storage on "
                             f"{self.server.name}: http status {status}")
        except HTTPResponseError as error:
            logger.exception(f"Storage: could not remove distributor node {node_base_url} on storage on "
                             f"{self.server.name}: http error")

        # Ignore errors here. The watchdog will try to remove the dist node later again.


class DistributorNode(NodeTypeBase):
    def __init__(self, loop=None):
        super().__init__()
        self.bound_to_storage_node_base_url: str = ''
        self.tx_current_rate: int = 0  # in Mbit/s
        self.tx_max_rate: int = 0  # in Mbit/s
        self.rx_current_rate: int = 0  # in Mbit/s
        self.tx_total: int = 0  # in MB
        self.rx_total: int = 0  # in MB
        self.current_connections: Optional[int] = 0  # HTTP connections serving videos
        self.waiting_clients: int = 0  # number of clients waiting for a file being downloaded
        self.files_total_size: int = 0  # in MB
        self.files_count: int = 0
        self.free_space: int = 0  # in MB
        self.copy_files_status: List[DistributorCopyFileStatus] = []

        self.tx_load: float = 0.0  # tx_current_rate / tx_max_rate

        self.first_request_done: Event = Event(loop=loop)
        self.storage_node: Optional[StorageNode] = None

    async def watchdog(self):
        status_url = f"{self.base_url}/api/distributor/status"
        first_request = True
        while True:
            try:
                ret: DistributorStatus
                status, ret = await HTTPClient.internal_request_manager('GET', status_url, None, DistributorStatus)
                if status == 200:
                    self.bound_to_storage_node_base_url = ret.bound_to_storage_node_base_url
                    self.tx_current_rate = ret.tx_current_rate
                    self.tx_max_rate = ret.tx_max_rate
                    self.rx_current_rate = ret.rx_current_rate
                    self.tx_total = ret.tx_total
                    self.rx_total = ret.rx_total
                    self.current_connections = ret.current_connections
                    self.waiting_clients = ret.waiting_clients
                    self.files_total_size = ret.files_total_size
                    self.files_count = ret.files_count
                    self.free_space = ret.free_space
                    self.copy_files_status = ret.copy_files_status
                    self.tx_load = self.tx_current_rate / self.tx_max_rate

                    if first_request:
                        logger.info(f"<Distributor watcher {self.server.name}> bound_to_storage="
                                    f"{self.bound_to_storage_node_base_url}, free_space={self.free_space} MB")
                        first_request = False
                        self.first_request_done.set()

                else:
                    logger.error(f"<Distributor watcher {self.server.name}> error: http status {status}")

            except HTTPResponseError:
                logger.exception(f"<Distributor watcher {self.server.name}> http error")
            await sleep(6)

    async def set_storage_node(self, node: StorageNode):
        self.storage_node = node
        node.add_dist_node(self)

    def remove_storage_node(self):
        node: Optional[StorageNode] = self.storage_node
        if node is None:
            return
        self.storage_node = None
        node.remove_dist_node(self.base_url)


#
# Exceptions
#

class EncoderCreateNewStreamError(Exception):
    pass


class ContentStartStreamingError(Exception):
    def __init__(self, content: ContentNode, stream: "ManagerStream"):
        self.content = content
        self.stream = stream
        super().__init__("Could not add stream to content node.")


class AddDistributorError(Exception):
    pass
