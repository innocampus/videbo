from distutils.util import strtobool
from logging import getLogger
from pathlib import Path
from typing import NoReturn, Optional, Union

from aiohttp.web_exceptions import (
    HTTPForbidden,
    HTTPInternalServerError,
    HTTPNotAcceptable,
    HTTPNotFound,
    HTTPOk,
)
from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from videbo import settings
from videbo.auth import ensure_auth
from videbo.exceptions import FFMpegError
from videbo.misc.functions import rel_path
from videbo.models import RequestJWTData, Role, TokenIssuer
from videbo.route_def import RouteTableDef
from videbo.temp_file import TempFile
from videbo.web import ensure_json_body, file_serve_headers, serve_file_via_x_accel
from videbo.storage.http_util import (
    get_video_payload,
    handle_thumbnail_request,
    save_temp_and_get_response,
    set_dist_node_state,
    verify_file_exists,
    video_check_redirect,
)
from videbo.storage.util import FileStorage, schedule_video_delete
from videbo.storage.exceptions import (
    BadFileExtension,
    FileTooBigError,
    FormFieldMissing,
)
from .models import *


log = getLogger(__name__)

routes = RouteTableDef()


@routes.get_with_cors(r'/api/upload/maxsize')
async def get_max_size(_: Request) -> Response:
    """Get max file size in MB."""
    return MaxSizeMB().json_response()


@routes.post_with_cors(r'/api/upload/file', allow_headers=['Authorization'])
@ensure_auth(Role.client)
async def upload_file(request: Request, jwt_data: UploadFileJWTData) -> Response:
    """
    Facilitates upload, analysis, and thumbnail creation for a video file.

    Assumes the request to come from a multipart form.
    Extracts the relevant form field, then creates a `TempFile` instance,
    loads the payload into that temporary file and analyzes it.
    If successful, the response data include URLs to both video and its
    generated thumbnails in temporary storage.

    To save them into permanent storage, the `save_file` route must be called.

    While the upload is taking place, the `FileStorage.num_current_uploads`
    counter is incremented by one.

    Args:
        request:
            The `aiohttp.web_request.Request` instance
        jwt_data:
            Decoded `UploadFileJWTData` from the token required for upload

    Returns:
        The `aiohttp.web_response.Response` instance returned by the
        `save_temp_and_get_response` coroutine.

    Raises:
        `HTTPNotAcceptable` (406)
            if the request content-type is not `multipart/form-data`
        `HTTPForbidden` (403)
            if the JWT field `is_allowed_to_upload_file` is `False`
        `HTTPInternalServerError` (500)
            if any exception is caught during `save_temp_and_get_response`
    """
    if request.content_type != "multipart/form-data":
        raise HTTPNotAcceptable(headers={"Accept": "multipart/form-data"})
    if not jwt_data.is_allowed_to_upload_file:
        log.info("Unauthorized upload request")
        raise HTTPForbidden()
    try:
        field = await get_video_payload(request, log=log)
    except (FormFieldMissing, BadFileExtension):
        return InvalidFormat().json_response(log=log)
    except FileTooBigError:
        return FileTooBig().json_response(log=log)
    storage = FileStorage.get_instance()
    storage.num_current_uploads += 1
    temp_file = TempFile.create(storage.temp_dir)
    # Any error should be followed by a cleanup of the temp. file
    try:
        return await save_temp_and_get_response(temp_file, field, log=log)
    except Exception as e:
        await temp_file.delete()
        if isinstance(e, FFMpegError):
            log.error(repr(e))
        else:
            log.exception(e)
        raise HTTPInternalServerError()
    finally:
        storage.num_current_uploads -= 1


@routes.get(r'/api/save/file/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_auth(Role.lms)
async def save_file(request: Request, jwt_data: SaveFileJWTData) -> Response:
    """
    Saves an uploaded video file (and its thumbnails) in permanent storage.

    Args:
        request:
            The `aiohttp.web_request.Request` instance
        jwt_data:
            Decoded `SaveFileJWTData` from the token required for saving

    Returns:
        The `aiohttp.web_response.Response` instance returned by the
        `aiohttp.web_response.json_response` function. The data is either
        `{"status": "ok"}` if the files were saved successfully or
        `{"status": "error", "error": "file_does_not_exist"}` if no file
        with the provided hash and extension was found in temporary storage.
        The latter case will have the status code 404.

    Raises:
        `HTTPForbidden` (403)
            if the JWT field `is_allowed_to_save_file` is `False`
    """
    if not jwt_data.is_allowed_to_save_file:
        log.info("Unauthorized save request")
        raise HTTPForbidden()
    hash_, ext = request.match_info["hash"], request.match_info["file_ext"]
    try:
        await FileStorage.get_instance().store_permanently(hash_, ext)
    except FileNotFoundError:
        return FileDoesNotExist(file_hash=hash_).json_response(log=log)
    return OK().json_response()


@routes.delete(r'/api/file/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_auth(Role.lms)
async def delete_file(request: Request, jwt_data: DeleteFileJWTData) -> Response:
    """
    Deletes a video file and its thumbnails from permanent storage.

    If permission is granted, deletion is scheduled, but the "OK"-response
    will be sent without waiting for the task to finish.

    IMPORTANT: Deletion may not occur, if any other connected LMS still uses
    the exact same file. This means an "OK"-response does not guarantee
    deletion of the file.

    Args:
        request:
            The `aiohttp.web_request.Request` instance
        jwt_data:
            Decoded `DeleteFileJWTData` from the token required for deletion

    Returns:
        The `aiohttp.web_response.Response` instance returned by
        `aiohttp.web_response.json_response` with the JSON `{"status": "ok"}`.

    Raises:
        `HTTPForbidden` (403)
            if the JWT field `is_allowed_to_delete_file` is `False`
    """
    if not jwt_data.is_allowed_to_delete_file:
        log.info("Unauthorized delete request")
        raise HTTPForbidden()
    origin = request.headers.getone("Origin", None)
    schedule_video_delete(request.match_info['hash'], request.match_info['file_ext'], origin=origin)
    return OK().json_response()


@routes.get(r'/file')
@ensure_auth(Role.client)
async def request_file(request: Request, jwt_data: RequestFileJWTData) -> Union[Response, FileResponse]:
    """
    Serves a video or a thumbnail (both from temporary or permanent storage).

    If the JWT issuer is external, the request may be redirected to a
    distributor node; if the `video_check_redirect` coroutine decides that it
    should in fact redirect, it raises a 302 status.

    If a thumbnail is requested, further handling is passed on to the
    `handle_thumbnail_request` coroutine.

    Thus, this function only returns a file serving response itself, if none
    of the above mentioned conditions are met. If configured accordingly,
    `X-Accel` capabilities will be used in that case.

    Args:
        request:
            The `aiohttp.web_request.Request` instance
        jwt_data:
            Decoded `RequestFileJWTData` from the token required for download

    Returns:
        A `aiohttp.web_fileresponse.FileResponse` instance with the actual
        video or thumbnail file, if no `X-Accel` support via the webserver
        is utilized; otherwise a regular `aiohttp.web_response.Response`
        instance with the configured `X-Accel`-parameters is returned.

    Raises:
        `HTTPNotFound` (404)
            if a permanently stored video was requested, but no video with
            the provided file hash and extension was found.
    """
    hash_, ext = jwt_data.hash, jwt_data.file_ext
    file_storage = FileStorage.get_instance()
    if jwt_data.type == FileType.VIDEO:
        try:
            path = file_storage.get_perm_video_path(hash_, ext)
        except FileNotFoundError:
            raise HTTPNotFound()
        # Don't consider redirecting internal requests:
        if jwt_data.iss != TokenIssuer.internal:
            stored_file = file_storage.get_file(hash_, ext)
            file_storage.distribution_controller.count_file_access(stored_file, jwt_data.rid)
            # May raise 302 redirect:
            await video_check_redirect(request, stored_file, log=log)
        log.debug(f"Serve video {hash_}")
    elif jwt_data.type == FileType.VIDEO_TEMP:
        path = file_storage.get_temp_video_path(hash_, ext)
        await verify_file_exists(path, log=log)  # no guarantee until the response!
        log.debug(f"Serve temp video {hash_}")
    else:  # must be FileType.THUMBNAIL or FileType.THUMBNAIL_TEMP
        return await handle_thumbnail_request(jwt_data, log=log)
    dl_name = request.query.get('downloadas')
    if not settings.webserver.x_accel_location:
        return FileResponse(path, headers=file_serve_headers(dl_name))
    uri = Path(settings.webserver.x_accel_location, rel_path(path.name))
    limit_rate = settings.webserver.get_x_accel_limit_rate(
        internal=jwt_data.iss == TokenIssuer.internal
    )
    return serve_file_via_x_accel(uri, limit_rate, download_filename=dl_name)


######################
# Admin-only routes: #
######################

@routes.post(r'/api/storage/distributor/add')
@ensure_auth(Role.admin)
@ensure_json_body
async def add_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    FileStorage.get_instance().distribution_controller.add_new_dist_node(data.base_url)
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/remove')
@ensure_auth(Role.admin)
@ensure_json_body
async def remove_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    await FileStorage.get_instance().distribution_controller.remove_dist_node(data.base_url)
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/disable')
@ensure_auth(Role.admin)
@ensure_json_body
async def disable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    set_dist_node_state(data.base_url, enabled=False, log=log)
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/enable')
@ensure_auth(Role.admin)
@ensure_json_body
async def enable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    set_dist_node_state(data.base_url, enabled=True, log=log)
    raise HTTPOk()


@routes.get(r'/api/storage/distributor/status')
@ensure_auth(Role.admin)
async def get_all_dist_nodes(_request: Request, _jwt_data: RequestJWTData) -> Response:
    nodes_statuses = FileStorage.get_instance().distribution_controller.get_nodes_status()
    return DistributorStatusDict(nodes=nodes_statuses).json_response()


@routes.get(r'/api/storage/status')
@ensure_auth(Role.admin)
async def get_status(_request: Request, _jwt_data: RequestJWTData) -> Response:
    storage_status = await FileStorage.get_instance().get_status()
    return storage_status.json_response()


@routes.get(r'/api/storage/files')
@ensure_auth(Role.admin)
async def get_files_list(request: Request, _jwt_data: RequestJWTData) -> Response:
    orphaned: Optional[bool] = None
    if request.query:
        orphaned_arg = request.query.get('orphaned')
        if orphaned_arg:
            orphaned = bool(strtobool(orphaned_arg.lower()))
    files = [
        StorageFileInfo.from_orm(file)
        async for file
        in FileStorage.get_instance().filtered_files(orphaned=orphaned)
    ]
    return StorageFilesList(files=files).json_response()


@routes.post(r'/api/storage/delete')
@ensure_auth(Role.admin)
@ensure_json_body
async def batch_delete_files(_request: Request, _jwt_data: RequestJWTData, data: DeleteFilesList) -> Response:
    storage = FileStorage.get_instance()
    not_deleted = list(await storage.remove_files(*data.hashes))
    if not_deleted:
        return NotAllFilesDeleted(not_deleted=not_deleted).json_response()
    return OK().json_response()
