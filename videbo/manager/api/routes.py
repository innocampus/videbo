import asyncio
import itertools
from aiohttp.web import Request, Response, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound
from videbo.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from videbo.web import json_response, ensure_json_body, ensure_no_reverse_proxy
from .models import CreateNewDistributorNodeParams, SetDistributorStatusParams, NodesStatusList, NodeStatus, \
    RemoveDistributorNodeParams
from videbo.manager import logger
from videbo.manager.cloud.definitions import DistributorInstanceDefinition
from videbo.manager.node_controller import NodeController
from videbo.manager.storage_controller import storage_controller
from videbo.manager.node_types import DistributorNode, StorageNode

routes = RouteTableDef()


@routes.get(r'/api/manager/admin/nodes')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
async def get_all_nodes(_request: Request, _jwt_data: BaseJWTData):
    nc = NodeController.get_instance()
    dist_nodes = await nc.get_all_nodes(DistributorNode)
    storage_nodes = await nc.get_all_nodes(StorageNode)

    nodes_list = []
    for node in itertools.chain(storage_nodes, dist_nodes):
        status = NodeStatus.construct()
        status.id = node.id
        status.type = type(node).__name__
        status.base_url = node.base_url
        status.created_manually = node.created_manually
        status.shutdown = node.shutdown
        status.enabled = node.enabled
        if node.server:
            status.name = node.server.name
            status.operational = node.server.is_operational()
        else:
            status.name = ""
            status.operational = False

        nodes_list.append(status)

    return json_response(NodesStatusList(nodes=nodes_list))


@routes.post(r'/api/manager/admin/create_distributor_node')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
@ensure_json_body()
async def create_distributor_node(_request: Request, _jwt_data: BaseJWTData, params: CreateNewDistributorNodeParams):
    nc = NodeController.get_instance()
    definition = nc.definitions.get_node_definition_with_name(params.definition_name)
    if definition is None:
        logger.error(f"create_distributor_node: could not find definition {params.definition_name}")
        raise HTTPNotFound()

    if not isinstance(definition, DistributorInstanceDefinition):
        logger.error(f"create_distributor_node: definition {params.definition_name} is not for a distributor node")
        raise HTTPNotFound()

    storage = storage_controller.get_storage_by_url(params.bound_to_storage_node_base_url)
    if storage is None:
        logger.error(f"create_distributor_node: could not find a storage with url "
                     f"{params.bound_to_storage_node_base_url}")
        raise HTTPNotFound()

    node = await nc.start_distributor_node(storage.base_url, definition=definition)
    node.created_manually = True

    return Response()


@routes.post(r'/api/manager/admin/set_distributor_node_status')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
@ensure_json_body()
async def set_distributor_node_status(_request: Request, _jwt_data: BaseJWTData, params: SetDistributorStatusParams):
    nc = NodeController.get_instance()
    nodes = await nc.get_all_nodes(DistributorNode)
    for node in nodes:
        if node.server and node.server.name == params.node_name:
            if params.enable:
                storage_controller.enable_distributor_node(node)
            else:
                storage_controller.disable_distributor_node(node)
            return Response()

    raise HTTPNotFound()


@routes.post(r'/api/manager/admin/remove_distributor_node')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
@ensure_json_body()
async def remove_distributor_node(_request: Request, _jwt_data: BaseJWTData, params: RemoveDistributorNodeParams):
    nc = NodeController.get_instance()
    nodes = await nc.get_all_nodes(DistributorNode)
    for node in nodes:
        if node.server and node.server.name == params.node_name:
            node.shutdown = True
            if node.enabled:
                logger.info(f"Disable node {node.server.name} ({node.base_url}) before removing/shutting down the node")
                storage_controller.disable_distributor_node(node)
                await asyncio.sleep(45)

            logger.info(f"Shut down node {node.server.name} ({node.base_url})")
            nc.stop_node(node)
            return Response()

    raise HTTPNotFound()
