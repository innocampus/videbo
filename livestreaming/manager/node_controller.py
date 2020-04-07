from asyncio import Lock, create_task, gather, wait_for, CancelledError, sleep
from threading import Thread
from typing import Dict, Type, List, Optional, TypeVar
from urllib.parse import urlparse
import time

from livestreaming.web import ensure_url_does_not_end_with_slash
from .cloud import CombinedCloudAPI, NodeCreateError
from .cloud import init_node
from .cloud import DeploymentStatus, VmStatus
from .cloud.server import Server, StaticServer, DynamicServer
from .cloud.definitions import CloudInstanceDefsController, OrderedInstanceDefinitionsList
from .cloud.dns_api import DNSManager, get_dns_api_by_provider
from . import  ManagerSettings, logger
from .node_types import NodeTypeBase, ContentNode, EncoderNode, BrokerNode, StorageNode


Node_Type_T = TypeVar('Node_Type_T', bound=NodeTypeBase)


class NodeController:
    def __init__(self, manager_settings: ManagerSettings, definitions: CloudInstanceDefsController):
        self.manager_settings: ManagerSettings = manager_settings
        self.definitions: CloudInstanceDefsController = definitions
        self.api: CombinedCloudAPI = CombinedCloudAPI(definitions)
        self.node_startup_lock: Lock = Lock()
        self.nodes: Dict[Type[NodeTypeBase], List[NodeTypeBase]] = {
            EncoderNode: [],
            ContentNode: [],
            StorageNode: [],
        }
        self.broker_node: Optional[BrokerNode] = None
        self.server_by_name: Dict[str, Server] = {}
        self.node_by_id: Dict[int, NodeTypeBase] = {}
        self.dns_manager: Optional[DNSManager] = None
        if manager_settings.cloud_deployment:
            self.dns_manager = get_dns_api_by_provider(definitions)

    async def start(self) -> None:
        if self.dns_manager:
            await self.dns_manager.get_all_records() # fill record cache

        await self._init_static_nodes()
        await self._handle_orphaned_nodes()

    async def get_pending_nodes(self, node_type: Type[Node_Type_T]) -> List[Node_Type_T]:
        """Get all nodes that are currently being created or initialized."""
        # Wait for the lock as there might be a node being started right now (in start_content_node) and we might not
        # know yet which instance definition will be used.
        async with self.node_startup_lock:
            nodes = self.nodes[node_type]
            list = []
            for node in nodes:
                if isinstance(node.server, DynamicServer) and DeploymentStatus.state_pending(node.server.deployment_status):
                    list.append(node)
            return list

    def get_operating_nodes(self, node_type: Type[Node_Type_T]) -> List[Node_Type_T]:
        """Get all nodes that are operating well."""
        nodes = self.nodes[node_type]
        list = []
        for node in nodes:
            # TODO also consider the current status as reported by node
            if node.is_operational():
                list.append(node)
        return list

    async def start_content_node(self, clients: int) -> ContentNode:
        """Start a new content node that should be able to deal with at least the given clients."""
        list = self.definitions.get_matching_content_defs(clients)
        new_node = ContentNode()
        self.nodes[ContentNode].append(new_node)
        self.node_by_id[new_node.id] = new_node
        new_node.lifecycle_task = create_task(self._dynamic_server_lifecycle(list, new_node))
        return new_node

    async def _start_dynamic_server(self, ordered_defs: OrderedInstanceDefinitionsList) -> DynamicServer:
        if not self.manager_settings.cloud_deployment:
            raise CloudDeploymentDisabledError()

        for definition in ordered_defs:
            try:
                server = await self.api.create_node(definition)
                self.server_by_name[server.name] = server
                return server
            except NodeCreateError:
                pass

    async def _dynamic_server_lifecycle(self, ordered_defs: OrderedInstanceDefinitionsList, node: NodeTypeBase):
        """A task that manages the lifecycle of a dynamic server. It stops the server on an error."""
        try:
            async with self.node_startup_lock:
                node.server = await self._start_dynamic_server(ordered_defs)

            if node.server is None:
                logger.error("Could not create a new node")
                await self._stop_dynamic_server(node)
                return

            await wait_for(self.api.wait_node_running(node.server), 60)

            tasks = []
            if self.dns_manager:
                tasks.append(node.server.set_domain_and_records(self.dns_manager))
            tasks.append(init_node(node.server))
            await gather(*tasks)

            node.base_url = "https://" + node.server.domain

            # Hand over to node watchdog and stay there until the node should be stopped.
            if node.server.deployment_status == DeploymentStatus.OPERATIONAL:
                logger.info(f"New {type(node).__name__} is operational, host {node.server.host}, url {node.base_url}")
                await node.watchdog()

        except Exception as e:
            if not isinstance(e, CancelledError):
                logger.exception(f"Exception in dynamic server lifecycle task (server url {node.base_url})")

        finally:
            # Stop server when this task ends.
            await self._stop_dynamic_server(node)
            if self.dns_manager and node.server and node.server.dns_record:
                await self.dns_manager.remove_record(node.server.dns_record)

    def stop_node(self, node: NodeTypeBase):
        if node.lifecycle_task:
            node.lifecycle_task.cancel()

    async def _stop_dynamic_server(self, node: NodeTypeBase) -> None:
        self.nodes[type(node)].remove(node)
        self.node_by_id.pop(node.id)
        if node.server and isinstance(node.server, DynamicServer):
            await self.api.delete_node(node.server)
            self.server_by_name.pop(node.server.name)

    async def _init_static_nodes_of_type(self, node_type: Type[NodeTypeBase], urls: str):
        node_urls = list(map(str.strip, urls.split(',')))
        for url in node_urls:
            url_parsed = urlparse(url)
            node = node_type()
            node.base_url = ensure_url_does_not_end_with_slash(url)
            node.server = StaticServer(url_parsed.netloc, url_parsed.hostname)
            if node_type is BrokerNode:
                self.broker_node = node
            else:
                self.nodes[node_type].append(node)
            self.node_by_id[node.id] = node
            self.server_by_name[node.server.name] = node.server
            node.lifecycle_task = create_task(self._static_server_lifecycle(node))

    async def _init_static_nodes(self) -> None:
        if ',' in self.manager_settings.static_broker_node_base_url:
            logger.critical(f"Config file: static_broker_node_base_url must not contain multiple urls")
            raise BrokerNodeNotUnique()

        await self._init_static_nodes_of_type(BrokerNode, self.manager_settings.static_broker_node_base_url)
        await self._init_static_nodes_of_type(ContentNode, self.manager_settings.static_content_node_base_urls)
        await self._init_static_nodes_of_type(EncoderNode, self.manager_settings.static_encoder_node_base_urls)
        await self._init_static_nodes_of_type(StorageNode, self.manager_settings.static_storage_node_base_url)

    async def _static_server_lifecycle(self, node: NodeTypeBase):
        """A task that manages the lifecycle of a static server. It tries to reach a server forever."""
        while True:
            try:
                await node.watchdog()
            except CancelledError:
                return
            except Exception as e:
                logger.exception(f"Error in static server lifecycle (type {type(node).__name__}, "
                                 f"server {node.server.name})")
                await sleep(20)

    async def _handle_orphaned_nodes(self) -> None:
        if not self.manager_settings.cloud_deployment:
            return

        orphaned_nodes = await self.api.get_all_nodes()
        for server in orphaned_nodes:
            server.deployment_status = DeploymentStatus.ORPHANED
            logger.warn(f"Found orphaned node: {server}")
            if self.manager_settings.remove_orphaned_nodes:
                self.api.delete_node(server)


class CloudDeploymentDisabledError(Exception):
    pass

class BrokerNodeNotUnique(Exception):
    pass