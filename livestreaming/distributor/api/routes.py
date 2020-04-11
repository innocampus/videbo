import asyncio
from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound, HTTPNotAcceptable, HTTPOk, HTTPInternalServerError
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.misc import get_free_disk_space
from livestreaming.network import NetworkInterfaces
from livestreaming.web import json_response, ensure_json_body
from livestreaming.storage.api.models import RequestFileJWTData, FileType
from livestreaming.storage.util import HashedVideoFile
from livestreaming.distributor import logger, distributor_settings
from livestreaming.distributor.files import file_controller
from .models import DistributorStatus, DistributorCopyFile, DistributorDeleteFiles, DistributorDeleteFilesResponse

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_jwt_data_and_role(Role.node)
async def get_status(request: Request, _jwt_data: BaseJWTData):
    status = DistributorStatus.construct()
    status.bound_to_storage_node_base_url = distributor_settings.bound_to_storage_base_url

    if 'get_connections' in request.query:
        status.current_connections = 0

    status.space_free = await file_controller.get_free_space()

    status.tx_max_rate = distributor_settings.tx_max_rate_mbit
    network = NetworkInterfaces.get_instance()
    interfaces = network.get_interface_names()
    if len(interfaces) > 0:
        # Just take the first network interface.
        iface = network.get_interface(interfaces[0])
        status.tx_current_rate = int(iface.tx_throughput * 8 / 1_000_000)
        status.rx_current_rate = int(iface.rx_throughput * 8 / 1_000_000)
        status.tx_total = int(iface.tx_bytes / 1024 / 1024)
        status.rx_total = int(iface.rx_bytes / 1024 / 1024)
    else:
        status.tx_current_rate = 0
        status.rx_current_rate = 0
        status.tx_total = 0
        status.rx_total = 0
        logger.error("No network interface found!")

    return json_response(status)


@routes.post(r'/api/distributor/copy/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_jwt_data_and_role(Role.node)
async def copy_file(request: Request, _jwt_data: BaseJWTData, data: DistributorCopyFile):
    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
    await file_controller.copy_file(file, data.url)
    raise HTTPOk()


@routes.post(r'/api/distributor/delete')
@ensure_jwt_data_and_role(Role.node)
@ensure_json_body()
async def delete_files(_request: Request, _jwt_data: BaseJWTData, data: DistributorDeleteFiles):
    for file_hash, file_ext in data.files:
        file = HashedVideoFile(file_hash, file_ext)
        await file_controller.delete_file(file)

    space_free = await file_controller.get_free_space()
    resp = DistributorDeleteFilesResponse(space_free=space_free)
    return json_response(resp)


@routes.get('/file')
@ensure_jwt_data_and_role(Role.client)
async def request_file(_: Request, jwt_data: RequestFileJWTData):
    if jwt_data.type != FileType.VIDEO:
        raise HTTPNotFound()

    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    try:
        await file_controller.file_exists(video, 60)
    except asyncio.TimeoutError:
        raise HTTPInternalServerError()

    # TODO
