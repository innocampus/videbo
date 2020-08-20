import asyncio
import urllib.parse
from aiohttp.web import Request, Response, RouteTableDef, FileResponse
from aiohttp.web_exceptions import HTTPNotFound, HTTPOk, HTTPServiceUnavailable, \
    HTTPInternalServerError
from time import time
from typing import List, Tuple
from videbo.auth import Role, BaseJWTData, ensure_jwt_data_and_role, JWT_ISS_INTERNAL
from videbo.network import NetworkInterfaces
from videbo.video import get_content_type_for_video
from videbo.web import json_response, ensure_json_body
from videbo.misc import sanitize_filename
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.storage.util import HashedVideoFile
from videbo.distributor import logger, distributor_settings
from videbo.distributor.files import file_controller, TooManyWaitingClients, NoSuchFile
from .models import DistributorStatus, DistributorCopyFile, DistributorDeleteFiles, DistributorDeleteFilesResponse,\
    DistributorFileList, DistributorCopyFileStatus

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_jwt_data_and_role(Role.node)
async def get_status(request: Request, _jwt_data: BaseJWTData):
    status = DistributorStatus.construct()
    status.bound_to_storage_node_base_url = distributor_settings.bound_to_storage_base_url

    # general information
    status.free_space = await file_controller.get_free_space()
    status.files_count = len(file_controller.files)
    status.files_total_size = int(file_controller.files_total_size / 1024 / 1024)
    status.waiting_clients = file_controller.waiting

    # network information
    status.tx_max_rate = distributor_settings.tx_max_rate_mbit
    network = NetworkInterfaces.get_instance()
    interfaces = network.get_interface_names()

    if distributor_settings.server_status_page:
        status.current_connections = network.get_server_status()

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

    # files being copied right now
    copy_files_status = []
    for file in file_controller.files_being_copied:
        duration = time() - file.copy_status.started
        copy_status = DistributorCopyFileStatus(hash=file.hash, file_ext=file.file_extension,
                                                loaded=file.copy_status.loaded_bytes,
                                                file_size=file.file_size,
                                                duration=duration)
        copy_files_status.append(copy_status)
    status.copy_files_status = copy_files_status

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
    new_file = file_controller.copy_file(file, data.from_base_url, data.file_size)
    if new_file.copy_status:
        await new_file.copy_status.wait_for(3600)
        # Recheck that file really exists now.
        video = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
        if not (await file_controller.file_exists(video, 1)):
            raise HTTPInternalServerError()
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
async def request_file(request: Request, jwt_data: RequestFileJWTData):
    if jwt_data.type != FileType.VIDEO:
        logger.info(f"Invalid request type: {jwt_data.type}")
        raise HTTPNotFound()
    file_hash, file_ext = jwt_data.hash, jwt_data.file_ext
    try:
        video = await file_controller.get_file(file_hash)
    except NoSuchFile:
        logger.info(f"Requested file that does not exist on this node: {file_hash}")
        raise HTTPNotFound()
    except asyncio.TimeoutError:
        logger.info(f"Waited for file, but timeout reached, file {file_hash}")
        raise HTTPServiceUnavailable()
    except TooManyWaitingClients:
        logger.info(f"Too many waiting users, file {file_hash}")
        raise HTTPServiceUnavailable()
    video.last_requested = int(time())

    headers = {
        "Cache-Control": "private, max-age=50400",
    }

    if "downloadas" in request.query:
        filename = sanitize_filename(request.query["downloadas"])
        filename = urllib.parse.quote(filename)
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    if jwt_data.iss != JWT_ISS_INTERNAL:
        # Check rate limit
        rate_limit = distributor_settings.nginx_x_accel_limit_rate_kbit
        if rate_limit:
            headers["X-Accel-Limit-Rate"] = str(int(rate_limit * 1024 / 8))

    path = file_controller.get_path_or_nginx_redirect(video)
    if isinstance(path, str):
        headers["X-Accel-Redirect"] = path
        content_type = get_content_type_for_video(file_ext)
        return Response(headers=headers, content_type=content_type)

    return FileResponse(path, headers=headers)
