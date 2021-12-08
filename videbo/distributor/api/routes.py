import asyncio
from time import time
from pathlib import Path
from typing import List, Tuple

from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound, HTTPOk, HTTPServiceUnavailable, HTTPInternalServerError

from videbo.settings import settings
from videbo.auth import Role, BaseJWTData, ensure_jwt_data_and_role, JWT_ISS_INTERNAL
from videbo.misc import MEGA, rel_path
from videbo.network import NetworkInterfaces
from videbo.web import json_response, ensure_json_body, file_serve_response
from videbo.storage.util import HashedVideoFile
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.distributor.files import file_controller, TooManyWaitingClients, NoSuchFile, NotSafeToDelete
from .models import DistributorStatus, DistributorCopyFile, DistributorDeleteFiles, DistributorDeleteFilesResponse,\
    DistributorFileList
from videbo.distributor import logger


routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_jwt_data_and_role(Role.node)
async def get_status(_request: Request, _jwt_data: BaseJWTData):
    status = DistributorStatus.construct()
    # Same attributes for storage and distributor nodes:
    status.files_total_size = int(file_controller.files_total_size / MEGA)
    status.files_count = len(file_controller.files)
    status.free_space = await file_controller.get_free_space()
    status.tx_max_rate = settings.tx_max_rate_mbit
    NetworkInterfaces.get_instance().update_node_status(status, settings.server_status_page, logger)
    # Specific to distributor nodes:
    status.copy_files_status = file_controller.get_copy_file_status()
    status.waiting_clients = file_controller.waiting
    status.bound_to_storage_node_base_url = settings.bound_to_storage_base_url
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
        if not (await file_controller.file_exists(request.match_info['hash'], 1)):
            raise HTTPInternalServerError()
    raise HTTPOk()


@routes.post(r'/api/distributor/delete')
@ensure_jwt_data_and_role(Role.node)
@ensure_json_body()
async def delete_files(_request: Request, _jwt_data: BaseJWTData, data: DistributorDeleteFiles):
    files_skipped: List[Tuple[str, str]] = []
    for file_hash, file_ext in data.files:
        try:
            await file_controller.delete_file(file_hash, safe=data.safe)
        except (NoSuchFile, NotSafeToDelete):
            files_skipped.append((file_hash, file_ext))
    free_space = await file_controller.get_free_space()
    resp = DistributorDeleteFilesResponse(files_skipped=files_skipped, free_space=free_space)
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
    x_accel = bool(settings.nginx_x_accel_location)
    if x_accel:
        path = Path(settings.nginx_x_accel_location, rel_path(str(video)))
    else:
        path = file_controller.get_path(video)
    dl = request.query.get('downloadas')
    limit_rate = float(jwt_data.iss != JWT_ISS_INTERNAL and settings.nginx_x_accel_limit_rate_mbit)
    return file_serve_response(path, x_accel, dl, limit_rate)
