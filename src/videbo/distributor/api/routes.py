import asyncio
import logging
from time import time
from pathlib import Path
from typing import NoReturn, Union

from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound, HTTPOk, HTTPServiceUnavailable, HTTPInternalServerError
from aiohttp.web_response import Response
from aiohttp.web_fileresponse import FileResponse

from videbo import settings
from videbo.auth import ensure_auth
from videbo.misc import MEGA
from videbo.misc.functions import rel_path
from videbo.models import HashedFilesList, Role, RequestJWTData, TokenIssuer
from videbo.network import NetworkInterfaces
from videbo.web import ensure_json_body, file_serve_headers, serve_file_via_x_accel
from ...hashed_file import HashedFile
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.distributor.files import DistributorFileController, TooManyWaitingClients, NoSuchFile, NotSafeToDelete
from .models import *


log = logging.getLogger(__name__)

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_auth(Role.node)
async def get_status(_request: Request, _jwt_data: RequestJWTData) -> Response:
    file_controller = DistributorFileController.get_instance()
    status = DistributorStatus(
        tx_current_rate=0,
        rx_current_rate=0,
        tx_total=0,
        rx_total=0,
        tx_max_rate=settings.tx_max_rate_mbit,
        files_total_size=file_controller.files_total_size / MEGA,
        files_count=len(file_controller.files),
        free_space=await file_controller.get_free_space(),
        bound_to_storage_node_base_url=settings.public_base_url,
        waiting_clients=file_controller.waiting,
        copy_files_status=file_controller.get_copy_file_status(),
    )
    NetworkInterfaces.get_instance().update_node_status(status, logger=log)
    return status.json_response()


@routes.get(r'/api/distributor/files')
@ensure_auth(Role.node)
async def get_all_files(_request: Request, _jwt_data: RequestJWTData) -> Response:
    return DistributorFileList.parse_obj({
        "files": DistributorFileController.get_instance().files.values()
    }).json_response()


@routes.post(r'/api/distributor/copy/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_auth(Role.node)
@ensure_json_body
async def copy_file(request: Request, _jwt_data: RequestJWTData, data: DistributorCopyFile) -> NoReturn:
    file_controller = DistributorFileController.get_instance()
    file = HashedFile(request.match_info['hash'], request.match_info['file_ext'])
    new_file = file_controller.copy_file(file, data.from_base_url, data.file_size)
    if new_file.copy_status:
        await new_file.copy_status.wait_for(3600)
        # Recheck that file really exists now.
        if not (await file_controller.file_exists(request.match_info['hash'], 1)):
            raise HTTPInternalServerError()
    raise HTTPOk()


@routes.post(r'/api/distributor/delete')
@ensure_auth(Role.node)
@ensure_json_body
async def delete_files(_request: Request, _jwt_data: RequestJWTData, data: DistributorDeleteFiles) -> Response:
    file_controller = DistributorFileController.get_instance()
    files_skipped: HashedFilesList = []
    for file in data.files:
        try:
            await file_controller.delete_file(file.hash, safe=data.safe)
        except (NoSuchFile, NotSafeToDelete):
            files_skipped.append(file)
    free_space = await file_controller.get_free_space()
    return DistributorDeleteFilesResponse(files_skipped=files_skipped, free_space=free_space).json_response()


@routes.get(r'/file')
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
    download_filename = request.query.get('downloadas')
    if not settings.webserver.x_accel_location:
        return FileResponse(
            file_controller.get_path(video),
            headers=file_serve_headers(download_filename),
        )
    uri = Path(settings.webserver.x_accel_location, rel_path(str(video)))
    limit_rate = settings.webserver.get_x_accel_limit_rate(
        internal=jwt_data.iss == TokenIssuer.internal
    )
    return serve_file_via_x_accel(uri, limit_rate, download_filename)
