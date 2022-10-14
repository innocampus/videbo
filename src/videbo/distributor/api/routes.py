import asyncio
import logging
from time import time
from pathlib import Path
from typing import Union

from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound, HTTPOk, HTTPServiceUnavailable, HTTPInternalServerError
from aiohttp.web_response import Response
from aiohttp.web_fileresponse import FileResponse

from videbo import settings
from videbo.auth import ensure_auth
from videbo.misc import MEGA
from videbo.misc.functions import rel_path
from videbo.models import TokenIssuer, Role, RequestJWTData
from videbo.network import NetworkInterfaces
from videbo.web import ensure_json_body, file_serve_response
from videbo.storage.util import HashedVideoFile
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.distributor.files import DistributorFileController, TooManyWaitingClients, NoSuchFile, NotSafeToDelete
from .models import *


log = logging.getLogger(__name__)

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')  # type: ignore[arg-type]
@ensure_auth(Role.node)
async def get_status(_request: Request, _jwt_data: RequestJWTData) -> Response:
    file_controller = DistributorFileController.get_instance()
    status = DistributorStatus.construct()
    # Same attributes for storage and distributor nodes:
    status.files_total_size = int(file_controller.files_total_size / MEGA)
    status.files_count = len(file_controller.files)
    status.free_space = await file_controller.get_free_space()
    status.tx_max_rate = settings.tx_max_rate_mbit
    NetworkInterfaces.get_instance().update_node_status(status, logger=log)
    # Specific to distributor nodes:
    status.copy_files_status = file_controller.get_copy_file_status()
    status.waiting_clients = file_controller.waiting
    status.bound_to_storage_node_base_url = settings.public_base_url
    return status.json_response()


@routes.get(r'/api/distributor/files')  # type: ignore[arg-type]
@ensure_auth(Role.node)
async def get_all_files(_request: Request, _jwt_data: RequestJWTData) -> Response:
    all_files: list[tuple[str, str]] = []
    for file in DistributorFileController.get_instance().files.values():
        all_files.append((file.hash, file.file_ext))
    return DistributorFileList(files=all_files).json_response()


@routes.post(r'/api/distributor/copy/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')  # type: ignore[arg-type]
@ensure_auth(Role.node)
@ensure_json_body
async def copy_file(request: Request, _jwt_data: RequestJWTData, data: DistributorCopyFile) -> None:
    file_controller = DistributorFileController.get_instance()
    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
    new_file = file_controller.copy_file(file, data.from_base_url, data.file_size)
    if new_file.copy_status:
        await new_file.copy_status.wait_for(3600)
        # Recheck that file really exists now.
        if not (await file_controller.file_exists(request.match_info['hash'], 1)):
            raise HTTPInternalServerError()
    raise HTTPOk()


@routes.post(r'/api/distributor/delete')  # type: ignore[arg-type]
@ensure_auth(Role.node)
@ensure_json_body
async def delete_files(_request: Request, _jwt_data: RequestJWTData, data: DistributorDeleteFiles) -> Response:
    file_controller = DistributorFileController.get_instance()
    files_skipped: list[tuple[str, str]] = []
    for file_hash, file_ext in data.files:
        try:
            await file_controller.delete_file(file_hash, safe=data.safe)
        except (NoSuchFile, NotSafeToDelete):
            files_skipped.append((file_hash, file_ext))
    free_space = await file_controller.get_free_space()
    return DistributorDeleteFilesResponse(files_skipped=files_skipped, free_space=free_space).json_response()


@routes.get('/file')  # type: ignore[arg-type]
@ensure_auth(Role.client)
async def request_file(request: Request, jwt_data: RequestFileJWTData) -> Union[Response, FileResponse]:
    file_controller = DistributorFileController.get_instance()
    if jwt_data.type != FileType.VIDEO:
        log.info(f"Invalid request type: {jwt_data.type}")
        raise HTTPNotFound()
    file_hash = jwt_data.hash
    try:
        video = await file_controller.get_file(file_hash)
    except NoSuchFile:
        log.info(f"Requested file that does not exist on this node: {file_hash}")
        raise HTTPNotFound()
    except asyncio.TimeoutError:
        log.info(f"Waited for file, but timeout reached, file {file_hash}")
        raise HTTPServiceUnavailable()
    except TooManyWaitingClients:
        log.info(f"Too many waiting users, file {file_hash}")
        raise HTTPServiceUnavailable()
    video.last_requested = int(time())
    if settings.webserver.x_accel_location:
        path = Path(settings.webserver.x_accel_location, rel_path(str(video)))
    else:
        path = file_controller.get_path(video)
    dl = request.query.get('downloadas')
    # The 'X-Accel-Limit-Rate' header value should be non-zero, only if the request is not internal:
    limit_rate = float(jwt_data.iss != TokenIssuer.internal and settings.webserver.x_accel_limit_rate_mbit)
    return file_serve_response(path, bool(settings.webserver.x_accel_location), dl, limit_rate)
