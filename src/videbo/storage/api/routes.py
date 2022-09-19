import logging
import asyncio
import urllib.parse
from collections.abc import Iterator
from distutils.util import strtobool
from pathlib import Path
from time import time
from typing import Optional, Union

from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response
from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_routedef import RouteTableDef
from aiohttp.multipart import BodyPartReader
from aiohttp.web_exceptions import HTTPOk  # 2xx
from aiohttp.web_exceptions import HTTPFound  # 3xx
from aiohttp.web_exceptions import (HTTPBadRequest, HTTPForbidden, HTTPNotFound, HTTPNotAcceptable, HTTPConflict,
                                    HTTPGone)  # 4xx
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPServiceUnavailable  # 5xx

from videbo import storage_settings as settings
from videbo.auth import ensure_auth
from videbo.exceptions import InvalidMimeTypeError, InvalidVideoError, FFProbeError
from videbo.misc import MEGA
from videbo.misc.functions import rel_path
from videbo.models import RequestJWTData, Role, TokenIssuer
from videbo.network import NetworkInterfaces
from videbo.video import VideoInfo, VideoValidator, VideoConfig
from videbo.web import ensure_json_body, file_serve_headers, file_serve_response, route_with_cors
from videbo.storage.util import (FileStorage, JPG_EXT, HashedVideoFile, StoredHashedVideoFile, TempFile,
                                 is_allowed_file_ending, schedule_video_delete)
from videbo.storage.exceptions import (FileTooBigError, FormFieldMissing, BadFileExtension, UnknownDistURL,
                                       DistAlreadyDisabled, DistAlreadyEnabled)
from videbo.storage.distribution import DistributionNodeInfo
from .models import *


log = logging.getLogger(__name__)

routes = RouteTableDef()

EXTERNAL_JWT_LIFE_TIME = 3600  # 1 hour in seconds
CHUNK_SIZE_DEFAULT = 300 * 1024  # 300 KB in bytes
CONTENT_TYPES = {JPG_EXT: 'image/jpeg'}


def get_expiration_time(seconds_from_now: int = EXTERNAL_JWT_LIFE_TIME) -> int:
    return int(time() + seconds_from_now)


def generate_video_url(video: HashedVideoFile, temp: bool) -> str:
    jwt_data = RequestFileJWTData(
        exp=get_expiration_time(),
        iss=TokenIssuer.external,
        role=Role.client,
        type=FileType.VIDEO_TEMP if temp else FileType.VIDEO,
        hash=video.hash,
        file_ext=video.file_ext,
        rid=''
    )
    return f"{settings.public_base_url}/file?jwt={jwt_data.encode()}"


def generate_thumb_urls(video: HashedVideoFile, temp: bool, thumb_count: int) -> Iterator[str]:
    exp = get_expiration_time()
    for thumb_id in range(thumb_count):
        jwt_data = RequestFileJWTData(
            exp=exp,
            iss=TokenIssuer.external,
            role=Role.client,
            type=FileType.THUMBNAIL_TEMP if temp else FileType.THUMBNAIL,
            hash=video.hash,
            file_ext=video.file_ext,
            thumb_id=thumb_id,
            rid='',
        )
        yield f"{settings.public_base_url}/file?jwt={jwt_data.encode()}"


async def read_data(file: TempFile, field: BodyPartReader, chunk_size: int = CHUNK_SIZE_DEFAULT) -> None:
    """Save the video data into temporary file."""
    log.info("Start reading file from client")
    data = await field.read_chunk(chunk_size)
    while len(data) > 0:
        if file.size > settings.max_file_size_mb * MEGA:
            await file.delete()
            raise FileTooBigError()
        await file.write(data)
        data = await field.read_chunk(chunk_size)
    await file.close()
    log.info(f"File was uploaded ({file.size} Bytes)")


async def get_video_payload(request: Request) -> BodyPartReader:
    """Perform payload checks and extract the part containing the video file."""
    multipart = await request.multipart()
    # Skip to the field that we are interested in;
    # this cannot be another (nested) MultipartReader, but should be a BodyPartReader instance,
    # and the field name should be `video`.
    field = await multipart.next()
    while not isinstance(field, BodyPartReader) or field.name != 'video':
        if field is None:
            raise FormFieldMissing()
        field = await multipart.next()
    # Simple file extension check.
    if not is_allowed_file_ending(field.filename):
        log.warning(f"file ending not allowed ({field.filename})")
        raise BadFileExtension()
    # Check file/content size as reported in the header if supplied.
    if request.content_length and request.content_length > settings.max_file_size_mb * MEGA:
        raise FileTooBigError()
    return field


async def get_video_info(file: TempFile) -> VideoInfo:
    """Run checks with ffprobe and return info."""
    video = VideoInfo(video_file=file.path, video_config=VideoConfig())
    validator = VideoValidator(info=video)
    await video.fetch_mime_type()
    try:
        validator.check_valid_mime_type()
    except InvalidMimeTypeError as mimetype_err:
        log.warning(f"invalid video mime type found in request (mime type: {mimetype_err.mime_type}).")
        raise
    try:
        await video.fetch_info()
    except FFProbeError as ffprobe_err:
        log.warning("invalid video file found in request (ffprobe error, timeout=%i, stderr below).",
                    ffprobe_err.timeout)
        if ffprobe_err.stderr is not None:
            log.warning(ffprobe_err.stderr)
        raise
    try:
        validator.check_valid_video()
    except InvalidVideoError as video_err:
        log.warning("invalid video file found in request (video: %s, audio: %s, container: %s).",
                    video_err.video_codec, video_err.audio_codec, video_err.container)
        raise
    return video


def invalid_format_response() -> Response:
    log.warning("no or invalid file found in request.")
    return json_response({'error': 'invalid_format'}, status=415)


def file_too_big_response() -> Response:
    log.warning("client wanted to upload file that is too big.")
    return json_response({'max_size': settings.max_file_size_mb}, status=413)


async def save_temp_file(file: TempFile, video_info: VideoInfo) -> tuple[HashedVideoFile, int, float]:
    """Saves video in a temporary file and returns its wrapper, thumbnail count and the video duration."""
    video_duration = int(video_info.get_length())
    try:
        file_ext = video_info.get_suggested_file_extension()
    except InvalidVideoError:
        raise BadFileExtension()
    hashed_file = await file.persist(file_ext)
    file_storage = FileStorage.get_instance()
    video_info.video_file = file_storage.get_path_in_temp(hashed_file)
    log.info(f"saved temp file, size: {file.size}, duration: {video_duration}, hash: {hashed_file.hash}")
    thumb_count = await file_storage.generate_thumbs(hashed_file, video_info)
    log.info(f"saved {thumb_count} temp thumbnails for temp video, hash: {hashed_file.hash}")
    return hashed_file, thumb_count, video_duration


async def read_and_save_temp(file: TempFile, field: BodyPartReader) -> Response:
    """
    Reads and saves video payload in a temporary file and returns relevant data in a JSON response (status 200).
    May return specific error responses instead (other status codes).
    """
    try:
        await read_data(file, field)
    except FileTooBigError:
        await file.delete()
        return file_too_big_response()
    try:
        video = await get_video_info(file)
    except (InvalidMimeTypeError, FFProbeError, InvalidVideoError):
        await file.delete()
        return invalid_format_response()
    try:
        stored_file, thumb_count, duration = await save_temp_file(file, video)
    except BadFileExtension:
        await file.delete()
        return invalid_format_response()
    jwt_data = FileUploadedResponseJWT(
        exp=get_expiration_time(),
        iss=TokenIssuer.external,
        hash=stored_file.hash,
        file_ext=stored_file.file_ext,
        thumbnails_available=thumb_count,
        duration=duration
    )
    resp = {
        'result': 'ok',
        'jwt': jwt_data.encode(),
        'url': generate_video_url(stored_file, temp=True),
        'thumbnails': list(generate_thumb_urls(stored_file, temp=True, thumb_count=thumb_count)),
    }
    return json_response(resp)


@route_with_cors(routes, '/api/upload/maxsize', 'GET')
async def get_max_size(_: Request) -> Response:
    """Get max file size in mb."""
    return json_response({'max_size': settings.max_file_size_mb})


@route_with_cors(routes, '/api/upload/file', 'POST', allow_headers=['Authorization'])
@ensure_auth(Role.client)
async def upload_file(request: Request, jwt_token: UploadFileJWTData) -> Response:
    """User wants to upload a video."""
    if request.content_type != 'multipart/form-data':
        raise HTTPNotAcceptable(headers={'Accept': 'multipart/form-data'})
    # check JWT permission
    if not jwt_token.is_allowed_to_upload_file:
        raise HTTPForbidden()
    try:
        field = await get_video_payload(request)
    except (FormFieldMissing, BadFileExtension):
        return invalid_format_response()
    except FileTooBigError:
        return file_too_big_response()
    storage = FileStorage.get_instance()
    file: TempFile = storage.create_temp_file()
    storage.num_current_uploads += 1
    # Form this point on, any error should be followed by a cleanup of the temp. file
    try:
        return await read_and_save_temp(file, field)
    except Exception as e:
        await file.delete()
        log.exception(e)
        raise HTTPInternalServerError()
    finally:
        storage.num_current_uploads -= 1


@routes.get('/api/save/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')  # type: ignore[arg-type]
@ensure_auth(Role.lms)
async def save_file(request: Request, jwt_data: SaveFileJWTData) -> Response:
    """Confirms that the file should be saved permanently."""
    if not jwt_data.is_allowed_to_save_file:
        log.info('unauthorized request')
        raise HTTPForbidden()
    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
    try:
        file_storage = FileStorage.get_instance()
        await file_storage.add_file_from_temp(file)
        thumb_count = settings.thumb_suggestion_count
        await file_storage.add_thumbs_from_temp(file, thumb_count)
    except FileNotFoundError:
        log.error('Cannot save file with hash %s to video, file does not exist.', file.hash)
        return json_response({'status': 'error', 'error': 'file_does_not_exist'}, status=404)
    return json_response({'status': 'ok'})


@routes.delete('/api/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')  # type: ignore[arg-type]
@ensure_auth(Role.lms)
async def delete_file(request: Request, jwt_data: DeleteFileJWTData) -> Response:
    """Delete the file with the hash."""
    if not jwt_data.is_allowed_to_delete_file:
        log.info("unauthorized request")
        raise HTTPForbidden()

    origin = request.headers.getone("Origin", None)
    schedule_video_delete(request.match_info['hash'], request.match_info['file_ext'], origin)
    return json_response({"status": "ok"})  # always succeeds


@routes.get('/file')  # type: ignore[arg-type]
@ensure_auth(Role.client)
async def request_file(request: Request, jwt_data: RequestFileJWTData) -> Union[Response, FileResponse]:
    """
    Serve a video or a thumbnail.
    The JWT must contain a valid `type` attribute (as defined in the `FileType` Enum class) for the request to be
    processed at all.
    If an external request is made for a video file, the request may be redirected to a distributor node;
    if the `video_check_redirect` function decides that it should in fact redirect, a 302 status is raised,
    and the function is "interrupted".
    If a thumbnail is requested, the further handling is passed on to `handle_thumbnail_request`.
    Thus, this function only returns a file serving response itself, if none of the above mentioned conditions are met.
    If so configured, X-Accel capabilities will be used in that case.
    """
    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    file_storage = FileStorage.get_instance()
    if jwt_data.type == FileType.VIDEO:
        # Record the video access and find out if this node serves the file or should redirect to a distributor node.
        try:
            video = await file_storage.get_file(jwt_data.hash, jwt_data.file_ext)
        except FileNotFoundError:
            raise HTTPNotFound()
        # Only consider redirecting the client when it is an external request.
        if jwt_data.iss != TokenIssuer.internal:
            file_storage.distribution_controller.count_file_access(video, jwt_data.rid)
            await video_check_redirect(request, video)
        log.debug(f"serve video with hash {jwt_data.hash}")
        path = file_storage.get_path(video)
    elif jwt_data.type == FileType.VIDEO_TEMP:
        log.debug(f"serve temp video with hash {jwt_data.hash}")
        path = file_storage.get_path_in_temp(video)
    else:  # can only be a thumbnail request, since we check for a valid type in the beginning
        return await handle_thumbnail_request(jwt_data)
    # If a video file is requested we already know the file should exist.
    if jwt_data.type != FileType.VIDEO:
        await verify_file_exists(path)
    if settings.nginx_x_accel_location:
        path = Path(settings.nginx_x_accel_location, rel_path(str(video)))
    dl = request.query.get('downloadas')
    # The 'X-Accel-Limit-Rate' header value should be non-zero, only if the request is not internal:
    limit_rate = float(jwt_data.iss != TokenIssuer.internal and settings.nginx_x_accel_limit_rate_mbit)
    return file_serve_response(path, bool(settings.nginx_x_accel_location), dl, limit_rate)


async def video_check_redirect(request: Request, file: StoredHashedVideoFile) -> None:
    """
    This function attempts to find a distributor node that can serve the file and decides if redirection is in order.
    If no redirect should happen (i.e. the storage node should serve the file), this function simply returns.
    Otherwise either an error is raised, if all nodes are too busy, or a redirect to a distributor is issued.
    This function is also responsible for initiating distribution, i.e. copying the file to another node.
    """
    if request.http_range.start is not None and request.http_range.start > 0:
        return  # Do not redirect if the range header is present and the start is not zero
    own_tx_load = get_own_tx_load()
    node, has_complete_file = file.nodes.find_good_node(file)
    if node is None:
        # There is no distribution node.
        if file.views >= settings.copy_to_dist_views_threshold:
            if file.nodes.copying:
                # When we are here this means that there is no non-busy distribution node. Even the dist node that
                # is currently loading the file is too busy.
                log.info(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f} "
                         f"and waiting for copying to complete")
                raise HTTPServiceUnavailable()
            else:
                to_node = FileStorage.get_instance().distribution_controller.copy_file_to_one_node(file)
                if to_node is None:
                    if own_tx_load > 0.9:
                        # There is no dist node to copy the file to and this storage node is too busy.
                        log.warning(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f}")
                        raise HTTPServiceUnavailable()
                    else:
                        return  # Serve file
                else:
                    if own_tx_load > 0.5:
                        # Redirect to node where the client needs to wait until the node downloaded the file.
                        # Wait a moment to give distributor node time getting notified to copy the file.
                        await asyncio.sleep(1)
                        return video_redirect_to_node(request, to_node, file)
                    else:
                        return  # Serve file
        else:
            if own_tx_load > 0.9:
                # The file is not requested that often and this storage node is too busy.
                log.warning(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f}")
                raise HTTPServiceUnavailable()
            else:
                # This storage node is not too busy and can serve the file by itself.
                return
    elif has_complete_file:
        # One distribution node that can serve the file.
        return video_redirect_to_node(request, node, file)
    else:
        # There is only a distribution node that is downloading the file however.
        if own_tx_load > 0.5:
            # Redirect to node where the client needs to wait until the node downloaded the file.
            # Wait a moment to give distributor node time getting notified to copy the file.
            await asyncio.sleep(1)
            return video_redirect_to_node(request, node, file)
        else:
            return  # Serve file


def video_redirect_to_node(request: Request, node: DistributionNodeInfo, file: StoredHashedVideoFile) -> None:
    log.debug(f"Redirect user to {node.base_url} for video {file}")
    jwt = request.query['jwt']
    url = f"{node.base_url}/file?jwt={jwt}"
    downloadas = request.query.getone("downloadas", None)
    if downloadas:
        url += "&downloadas=" + urllib.parse.quote(downloadas)
    raise HTTPFound(url)


def get_own_tx_load() -> float:
    network = NetworkInterfaces.get_instance()
    tx_current_rate_mb = network.get_tx_current_rate()
    if tx_current_rate_mb is None:
        return 0.
    return tx_current_rate_mb / settings.tx_max_rate_mbit


async def verify_file_exists(path: Path) -> None:
    if not await asyncio.get_event_loop().run_in_executor(None, path.is_file):
        log.warning(f"file does not exist: {path}")
        raise HTTPNotFound()


async def handle_thumbnail_request(jwt_data: RequestFileJWTData) -> Response:
    """
    Returns a regular `Response` with the previously cached thumbnail data.
    Uses the `FileStorage` instance's capabilities of storing recently requested thumbnails in RAM.
    The usual sanity checks apply.
    """
    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    file_storage = FileStorage.get_instance()
    if jwt_data.thumb_id is None:
        log.info("thumb ID is None in JWT")
        raise HTTPBadRequest()
    if jwt_data.type == FileType.THUMBNAIL:
        log.debug(f"serve thumbnail {jwt_data.thumb_id} for video with hash {video}")
        path = file_storage.get_thumb_path(video, jwt_data.thumb_id)
    else:
        log.debug(f"serve temp thumbnail {jwt_data.thumb_id} for video with hash {video}")
        path = file_storage.get_thumb_path_in_temp(video, jwt_data.thumb_id)

    bytes_data: Optional[bytes] = file_storage.thumb_memory_cache.get(path)
    if bytes_data is None:
        def read_entire_file_and_close_it() -> bytes:
            with open(path, 'rb') as f:
                return f.read()
        try:
            bytes_data = await asyncio.get_event_loop().run_in_executor(None, read_entire_file_and_close_it)
        except FileNotFoundError:
            log.warning(f"file does not exist: {path}")
            raise HTTPNotFound()
        assert isinstance(bytes_data, bytes)

        # Setting cache maximum size to 0 effectively disables keeping the data:
        if settings.thumb_cache_max_mb > 0:
            file_storage.thumb_memory_cache[path] = bytes_data
    return Response(body=bytes_data, content_type=CONTENT_TYPES[path.suffix], headers=file_serve_headers())


@routes.post(r'/api/storage/distributor/add')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
@ensure_json_body
async def add_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> None:
    FileStorage.get_instance().distribution_controller.add_new_dist_node(data.base_url)
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/remove')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
@ensure_json_body
async def remove_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> None:
    await FileStorage.get_instance().distribution_controller.remove_dist_node(data.base_url)
    raise HTTPOk()


async def set_dist_node_state(base_url: str, enabled: bool) -> None:
    prefix = 'en' if enabled else 'dis'
    try:
        FileStorage.get_instance().distribution_controller.set_node_state(base_url, enabled=enabled)
    except UnknownDistURL:
        log.error(f"Request to {prefix}able unknown distributor node with URL `{base_url}`")
        raise HTTPGone()
    except (DistAlreadyDisabled, DistAlreadyEnabled):
        log.warning(f"Cannot to {prefix}able distributor node `{base_url}`; already {prefix}abled.")
        raise HTTPConflict()
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/disable')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
@ensure_json_body
async def disable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> None:
    await set_dist_node_state(data.base_url, enabled=False)


@routes.post(r'/api/storage/distributor/enable')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
@ensure_json_body
async def enable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> None:
    await set_dist_node_state(data.base_url, enabled=True)


@routes.get(r'/api/storage/distributor/status')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
async def get_all_dist_nodes(_request: Request, _jwt_data: RequestJWTData) -> Response:
    nodes_statuses = FileStorage.get_instance().distribution_controller.get_nodes_status()
    return DistributorStatusDict(nodes=nodes_statuses).json_response()


@routes.get(r'/api/storage/status')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
async def get_status(_request: Request, _jwt_data: RequestJWTData) -> Response:
    storage_status = await FileStorage.get_instance().get_status()
    return storage_status.json_response()


@routes.get(r'/api/storage/files')  # type: ignore[arg-type]
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


@routes.post('/api/storage/delete')  # type: ignore[arg-type]
@ensure_auth(Role.admin)
@ensure_json_body
async def batch_delete_files(_request: Request, _jwt_data: RequestJWTData, data: DeleteFilesList) -> Response:
    storage = FileStorage.get_instance()
    removed_list = await storage.remove_files(*data.hashes)
    results = zip(data.hashes, removed_list)
    not_deleted = [file_hash for file_hash, success in results if not success]
    if not_deleted:
        return json_response({'status': 'incomplete', 'not_deleted': not_deleted})
    return json_response({'status': 'ok'})
