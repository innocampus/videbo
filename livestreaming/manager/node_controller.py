from asyncio import Lock, create_task, gather, wait_for, CancelledError, sleep, Event, get_running_loop
from threading import Thread
from typing import Dict, Type, List, Optional, TypeVar
from urllib.parse import urlparse
import time

from livestreaming.web import ensure_url_does_not_end_with_slash
from livestreaming.misc import TaskManager
from .cloud import CombinedCloudAPI, NodeCreateError
from .cloud.cloud_deployment import init_node, remove_ssh_key
from .cloud import DeploymentStatus, VmStatus
from .cloud.server import Server, StaticServer, DynamicServer
from .cloud.definitions import CloudInstanceDefsController, OrderedInstanceDefinitionsList,\
    DistributorInstanceDefinition
from .cloud.dns_api import DNSManager, get_dns_api_by_provider
from . import  ManagerSettings, logger
from .node_types import NodeTypeBase, ContentNode, EncoderNode, BrokerNode, StorageNode, DistributorNode
from .db import Database

Node_Type_T = TypeVar('Node_Type_T', bound=NodeTypeBase)


class NodeController:
    _instance: Optional["NodeController"] = None

    def __init__(self, manager_settings: ManagerSettings, definitions: CloudInstanceDefsController,
                 db: Database):
        self.manager_settings: ManagerSettings = manager_settings
        self.definitions: CloudInstanceDefsController = definitions
        self.db: Database = db

        self.api: CombinedCloudAPI = CombinedCloudAPI(definitions)
        self.node_startup_delete_lock: Lock = Lock()
        self.nodes: Dict[Type[NodeTypeBase], List[NodeTypeBase]] = {
            EncoderNode: [],
            ContentNode: [],
            StorageNode: [],
            DistributorNode: [],
        }
        self.broker_node: Optional[BrokerNode] = None
        self.server_by_name: Dict[str, Server] = {}
        self.node_by_id: Dict[int, NodeTypeBase] = {}  # TODO Do we still need this???
        self.dns_manager: Optional[DNSManager] = None
        self.dyn_nodes_initialized = Event()

        if manager_settings.cloud_deployment:
            self.dns_manager = get_dns_api_by_provider(definitions)

    @classmethod
    def get_instance(cls) -> "NodeController":
        if cls._instance:
            return cls._instance
        raise NodeControllerNotInitializedError()

    @classmethod
    def init_instance(cls, manager_settings: ManagerSettings, definitions: CloudInstanceDefsController,
                 db: Database) -> "NodeController":
        cls._instance = NodeController(manager_settings, definitions, db)
        return cls._instance

    async def start(self) -> None:
        if self.dns_manager:
            await self.dns_manager.get_all_records() # fill record cache

        await self._init_static_nodes()
        await self._init_existing_and_handle_orphaned_nodes()
        task = create_task(self._handle_orphaned_servers_task())
        TaskManager.fire_and_forget_task(task)

    async def get_pending_nodes(self, node_type: Type[Node_Type_T]) -> List[Node_Type_T]:
        """Get all nodes that are currently being created or initialized."""
        # Wait for the lock as there might be a node being started right now (in start_content_node) and we might not
        # know yet which instance definition will be used.
        async with self.node_startup_delete_lock:
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

    async def get_all_nodes(self, node_type: Type[Node_Type_T]) -> List[Node_Type_T]:
        """Get all nodes of a type."""
        # Wait for the lock as there might be a node being started right now (in start_content_node) and we might not
        # know yet which instance definition will be used.
        async with self.node_startup_delete_lock:
            return self.nodes[node_type].copy()

    async def start_content_node(self, clients: int) -> ContentNode:
        """Start a new content node that should be able to deal with at least the given clients."""
        list = self.definitions.get_matching_content_defs(clients)
        new_node = ContentNode()
        self.nodes[ContentNode].append(new_node)
        self.node_by_id[new_node.id] = new_node
        new_node.lifecycle_task = create_task(self._dynamic_server_lifecycle(list, new_node))
        return new_node

    async def start_distributor_node(self, bound_to_storage_node_base_url: str, *,
                                     min_tx_rate_mbit: Optional[int] = None,
                                     definition: Optional[DistributorInstanceDefinition] = None) -> DistributorNode:
        """Start a new distributor node that should be able to deal with at least the given clients."""
        if min_tx_rate_mbit:
            list = self.definitions.get_matching_distributor_defs(min_tx_rate_mbit)
        elif definition:
            list = [definition]
        else:
            raise Exception("You need to pass min_tx_rate_mbit or definition!")

        new_node = DistributorNode()
        new_node.bound_to_storage_node_base_url = bound_to_storage_node_base_url
        self.nodes[DistributorNode].append(new_node)
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
                logger.info(f"Could not create node with definition {definition.section_name}")
                pass

    async def _dynamic_server_lifecycle(self, ordered_defs: Optional[OrderedInstanceDefinitionsList],
                                        node: NodeTypeBase):
        """A task that manages the lifecycle of a dynamic server. It stops the server on an error."""
        try:
            if node.server is None:
                # We need a server.
                if ordered_defs is None:
                    raise Exception("ordered_defs must not be None when there is no server")
                deployment_time_start = time.time()
                async with self.node_startup_delete_lock:
                    node.server = await self._start_dynamic_server(ordered_defs)

                if node.server is None:
                    logger.error("Could not create a new node")
                    # Node will be stopped/removed in finally clause.
                    return

                await wait_for(self.api.wait_node_running(node.server), 90)
                logger.info(f"Created node with definition {node.server.instance_definition.section_name}")

                tasks = []
                if self.dns_manager:
                    tasks.append(wait_for(node.server.set_domain_and_records(self.dns_manager), 90))
                tasks.append(wait_for(init_node(node), 8 * 60))
                await gather(*tasks)

                deployment_duration = time.time() - deployment_time_start
                logger.info(f"Ordering, booting and deployment of node {node.server.name} took "
                            f"{deployment_duration:.2f}s")
                node.base_url = "https://" + node.server.domain

                await self.db.save_node(node)

            # Hand over to node watchdog and stay there until the node should be stopped.
            if node.server.deployment_status == DeploymentStatus.OPERATIONAL:
                logger.info(f"New {type(node).__name__} is operational, host {node.server.host}, url {node.base_url}")
                await node.watchdog()
        except CancelledError:
            pass
        except Exception as e:
            logger.exception(f"Exception in dynamic server lifecycle task (server url {node.base_url})")
            node.shutdown = True  # We have no further use of the server.
        finally:
            # Stop server when this task ends.
            if node.shutdown:
                async with self.node_startup_delete_lock:
                    await self._stop_dynamic_server(node)

    def stop_node(self, node: NodeTypeBase):
        if node.lifecycle_task:
            node.lifecycle_task.cancel()

    async def _stop_dynamic_server(self, node: NodeTypeBase) -> None:
        if not self.node_startup_delete_lock.locked():
            logger.error("_stop_dynamic_server should only be called when node_startup_delete_lock is hold")

        try:
            self.nodes[type(node)].remove(node)
        except ValueError:
            pass
        self.node_by_id.pop(node.id, None)
        if node.server and isinstance(node.server, DynamicServer):
            logger.info(f"Stopping server {node.server.name}")
            self.server_by_name.pop(node.server.name, None)
            await self.api.delete_node(node.server)
            if self.dns_manager and node.server.dns_record:
                await self.dns_manager.remove_record(node.server.dns_record)
            await remove_ssh_key(node.server.host)
        await self.db.delete_node(node)

    async def _init_static_nodes_of_type(self, node_type: Type[NodeTypeBase], urls: str):
        node_urls = list(map(str.strip, urls.split(',')))
        for url in node_urls:
            url = url.strip()
            if len(url) == 0:
                continue

            url_parsed = urlparse(url)
            node = node_type()
            node.base_url = ensure_url_does_not_end_with_slash(url)
            node.server = StaticServer(url_parsed.netloc, url_parsed.hostname)
            async with self.node_startup_delete_lock:
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
        await self._init_static_nodes_of_type(DistributorNode, self.manager_settings.static_distributor_node_base_urls)

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

    async def _handle_orphaned_servers_task(self) -> None:
        """Periodically checks for orphaned cloud servers and stops them."""
        if not self.manager_settings.cloud_deployment:
            return

        while True:
            try:
                await sleep(10 * 60)

                # Use a lock to avoid race conditions when a node is getting started and the name was not yet saved.
                async with self.node_startup_delete_lock:
                    servers = await self.get_all_dynamic_servers()
                    for server in servers.values():
                        if server.name not in self.server_by_name:
                            server.deployment_status = DeploymentStatus.ORPHANED
                            logger.warn(f"Found orphaned server: {server}")
                            if self.manager_settings.remove_orphaned_nodes:
                                await self.api.delete_node(server)
            except CancelledError:
                raise
            except Exception:
                logger.exception("Error in _handle_orphaned_servers_task")

    async def get_all_dynamic_servers(self) -> Dict[str, DynamicServer]:
        ret_servers = {}
        servers = await self.api.get_all_nodes()
        for server in servers:
            # Only servers that belong to the namespace/prefix.
            if server.name.startswith(self.manager_settings.dynamic_node_name_prefix):
                existing_server_or_new = self.server_by_name.get(server.name, server)
                ret_servers[existing_server_or_new.name] = existing_server_or_new
        return ret_servers

    async def _init_existing_and_handle_orphaned_nodes(self):
        """Init existing nodes and remove all nodes that don't have a running server anymore."""
        servers_by_name = await self.get_all_dynamic_servers()
        nodes, orphaned_nodes = await self.db.get_nodes(servers_by_name, get_running_loop())

        for node in nodes:
            if not isinstance(node.server, DynamicServer):
                logger.error(f"Expected DynamicServer in _init_existing_and_handle_orphaned_nodes")

            async with self.node_startup_delete_lock:
                if not node.server.is_operational():
                    # Stop server and remove node.
                    await self._stop_dynamic_server(node)
                else:
                    if self.dns_manager:
                        # ensure domain records are set
                        await node.server.set_domain_and_records(self.dns_manager)
                    # start watchdog
                    self.nodes[type(node)].append(node)
                    self.node_by_id[node.id] = node
                    self.server_by_name[node.server.name] = node.server
                    node.lifecycle_task = create_task(self._dynamic_server_lifecycle(None, node))

        for orphaned_node in orphaned_nodes:
            logger.info(f"Found an orphaned node in database with id {orphaned_node.id} and "
                        f"url {orphaned_node.base_url}. There was no corresponding dynamic server found. Delete it.")
            await self.db.delete_node(orphaned_node)


class CloudDeploymentDisabledError(Exception):
    pass


class BrokerNodeNotUnique(Exception):
    pass


class NodeControllerNotInitializedError(Exception):
    pass
