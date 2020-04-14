import asyncio
from time import time
from typing import Optional, Dict
from livestreaming.misc import TaskManager
from .node_controller import NodeController
from .node_types import StorageNode, DistributorNode, AddDistributorError


class StorageDistributorController:
    def __init__(self):
        self.node_controller: Optional[NodeController] = None
        self.control_task: Optional[asyncio.Task] = None

    def init(self, nc: NodeController):
        self.node_controller = nc
        self.control_task = asyncio.create_task(self._control())
        TaskManager.fire_and_forget_task(self.control_task)

    async def _control(self):
        while True:
            dist_nodes = self.node_controller.get_operating_nodes(DistributorNode)
            storage_nodes = self.node_controller.get_operating_nodes(StorageNode)

            storage_by_base_url: Dict[str, StorageNode] = {}
            for node in storage_nodes:
                storage_by_base_url[node.base_url] = node

            # Check all dist nodes are connected to their storage.
            for node in dist_nodes:
                storage_node = storage_by_base_url.get(node.bound_to_storage_node_base_url)
                if node.storage_node is None and storage_node:
                    try:
                        await node.set_storage_node(storage_node)
                    except AddDistributorError:
                        pass

            for node in storage_nodes:
                await self._check_storage_dists_load(node)

            await asyncio.sleep(6)

    async def _check_storage_dists_load(self, storage: StorageNode):
        total_tx_current_rate = storage.tx_current_rate
        total_tx_max_rate = storage.tx_max_rate
        for dist in storage.dist_nodes.values():
            total_tx_current_rate += dist.tx_current_rate
            total_tx_max_rate += dist.tx_max_rate

        if total_tx_max_rate == 0:
            return

        total_tx_load = total_tx_current_rate / total_tx_max_rate
        if total_tx_load > 0.7:
            if storage.load_threshold_exceeded_since is None:
                storage.load_threshold_exceeded_since = time()
            elif (time() - storage.load_threshold_exceeded_since) > 45:
                # One more distribution node needed.
                pass # TODO
        else:
            storage.load_threshold_exceeded_since = None
