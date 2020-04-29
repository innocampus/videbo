import asyncio
import itertools
from aiohttp.web import Request, Response, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body, ensure_no_reverse_proxy
from livestreaming.manager.streams import stream_collection
from .models import LMSNewStreamReturn, LMSNewStreamParams, AllStreamsStatus, CreateNewDistributorNodeParams,\
    SetDistributorStatusParams, NodesStatusList, NodeStatus, RemoveDistributorNodeParams
from livestreaming.manager import logger
from livestreaming.manager.streams import StreamStateObserver, StreamState
from livestreaming.manager.cloud.definitions import DistributorInstanceDefinition
from livestreaming.manager.node_controller import NodeController
from livestreaming.manager.storage_controller import storage_controller
from livestreaming.manager.node_types import DistributorNode, StorageNode

routes = RouteTableDef()


@routes.post(r'/api/manager/stream/new')
@ensure_jwt_data_and_role(Role.lms)
@ensure_json_body()
async def new_stream(request: Request, jwt_data: BaseJWTData, json: LMSNewStreamParams):
    """LMS requests manager to set up a new stream."""
    try:
        stream = stream_collection.create_new_stream(json.ip_range, json.rtmps, json.lms_stream_instance_id,
                                                     json.expected_viewers)
        watcher = stream.state_observer.new_watcher()
        await StreamStateObserver.wait_until(watcher, StreamState.WAITING_FOR_CONNECTION)

        if stream.state == StreamState.NO_ENCODER_AVAILABLE:
            return_data = LMSNewStreamReturn(success=False, error="no_encoder_available")
            return json_response(return_data, status=503)

        if stream.state == StreamState.FFMPEG_ERROR:
            return_data = LMSNewStreamParams(success=False, error="ffmpeg_error")
            return json_response(return_data, status=500)

        if stream.state == StreamState.WAITING_FOR_CONNECTION:
            # This is the expected state.
            new_stream_data = stream.get_status(True)
            return_data = LMSNewStreamReturn(success=True, stream=new_stream_data)
            return json_response(return_data)

        return_data = LMSNewStreamReturn(success=False, error="unknown_error")
        return json_response(return_data, status=503)

    except Exception as e:
        error_type = type(e).__name__
        return_data = LMSNewStreamReturn(success=False, error=error_type)
        logger.exception("Exception in new_stream")
        return json_response(return_data, status=500)


@routes.get(r'/api/manager/stream/{stream_id:\d+}/status')
@ensure_jwt_data_and_role(Role.lms)
async def get_stream_status(request: Request, _jwt_data: BaseJWTData):
    try:
        stream_id = int(request.match_info['stream_id'])
        stream = stream_collection.get_stream_by_id(stream_id)
        return json_response(stream.get_status(True))
    except KeyError:
        raise HTTPNotFound()


@routes.get(r'/api/manager/stream/{stream_id:\d+}/stop')
@ensure_jwt_data_and_role(Role.lms)
async def stop_stream(request: Request, _jwt_data: BaseJWTData):
    """Forcefully stop a stream."""
    try:
        stream_id = int(request.match_info['stream_id'])
        stream = stream_collection.get_stream_by_id(stream_id)
        stream.stop()
        return Response()
    except KeyError:
        raise HTTPNotFound()


@routes.get(r'/api/manager/streams')
@ensure_jwt_data_and_role(Role.lms)
async def get_all_streams_status(request: Request, _jwt_data: BaseJWTData):
    try:
        streams_list = []
        for _, stream in stream_collection.streams.items():
            streams_list.append(stream.get_status(False))

        status = AllStreamsStatus(streams=streams_list)
        return json_response(status)
    except KeyError:
        raise HTTPNotFound()


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
