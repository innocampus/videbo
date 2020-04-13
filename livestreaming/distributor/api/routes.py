import asyncio
from aiohttp.web import Request, Response, RouteTableDef, FileResponse
from aiohttp.web_exceptions import HTTPNotFound, HTTPNotAcceptable, HTTPOk, HTTPServiceUnavailable
from typing import List, Tuple
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role, JWT_ISS_INTERNAL
from livestreaming.network import NetworkInterfaces
from livestreaming.web import json_response, ensure_json_body
from livestreaming.storage.api.models import RequestFileJWTData, FileType
from livestreaming.storage.util import HashedVideoFile
from livestreaming.distributor import logger, distributor_settings
from livestreaming.distributor.files import file_controller
from .models import DistributorStatus, DistributorCopyFile, DistributorDeleteFiles, DistributorDeleteFilesResponse,\
    DistributorFileList

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_jwt_data_and_role(Role.node)
async def get_status(request: Request, _jwt_data: BaseJWTData):
    status = DistributorStatus.construct()
    status.bound_to_storage_node_base_url = distributor_settings.bound_to_storage_base_url

    if 'get_connections' in request.query:
        status.current_connections = 0  # TODO

    status.free_space = await file_controller.get_free_space()

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


@routes.get(r'/api/distributor/files')
@ensure_jwt_data_and_role(Role.node)
async def get_all_files(_request: Request, _jwt_data: BaseJWTData):
    all_files: List[Tuple[str, str]] = []
    for file in file_controller.files.values():
        all_files.append((file.hash, file.file_extension))

    return json_response(DistributorFileList(files=all_files))


@routes.post(r'/api/distributor/copy/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_jwt_data_and_role(Role.node)
@ensure_json_body()
async def copy_file(request: Request, _jwt_data: BaseJWTData, data: DistributorCopyFile):
    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
    await file_controller.copy_file(file, data.from_base_url, data.file_size)
    raise HTTPOk()


@routes.post(r'/api/distributor/delete')
@ensure_jwt_data_and_role(Role.node)
@ensure_json_body()
async def delete_files(_request: Request, _jwt_data: BaseJWTData, data: DistributorDeleteFiles):
    for file_hash, file_ext in data.files:
        file = HashedVideoFile(file_hash, file_ext)
        await file_controller.delete_file(file)

    free_space = await file_controller.get_free_space()
    resp = DistributorDeleteFilesResponse(free_space=free_space)
    return json_response(resp)


@routes.get('/file')
@ensure_jwt_data_and_role(Role.client)
async def request_file(_: Request, jwt_data: RequestFileJWTData):
    if jwt_data.type != FileType.VIDEO:
        logger.info(f"Invalid request type: {jwt_data.type}")
        raise HTTPNotFound()

    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    try:
        if not (await file_controller.file_exists(video, 60)):
            logger.info(f"Requested file that does not exist on this node: {video.hash}")
            raise HTTPNotFound()
    except asyncio.TimeoutError:
        logger.info(f"Waited for file, but timeout reached, file {video.hash}")
        raise HTTPServiceUnavailable()

    headers = {
        "Cache-Control": "private, max-age=50400",
    }

    if jwt_data.iss != JWT_ISS_INTERNAL:
        # Check rate limit
        rate_limit = distributor_settings.nginx_x_accel_limit_rate_kbit
        if rate_limit:
            headers["X-Accel-Limit-Rate"] = rate_limit * 1024 / 8

    path = file_controller.get_path_or_nginx_redirect(video)
    if isinstance(path, str):
        headers["X-Accel-Redirect"] = path
        return Response(headers=headers)

    return FileResponse(path, headers=headers)
