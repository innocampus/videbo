import logging
import urllib.parse
from asyncio import sleep as async_sleep
from distutils.util import strtobool
from pathlib import Path
from typing import NoReturn, Optional, Union

from aiohttp.multipart import BodyPartReader
from aiohttp.web_exceptions import HTTPOk  # 2xx
from aiohttp.web_exceptions import HTTPFound  # 3xx
from aiohttp.web_exceptions import (HTTPBadRequest, HTTPForbidden, HTTPNotFound, HTTPNotAcceptable, HTTPConflict,
                                    HTTPGone)  # 4xx
from aiohttp.web_exceptions import HTTPInternalServerError, HTTPServiceUnavailable  # 5xx
from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import Response, json_response

from videbo import settings
from videbo.auth import ensure_auth, extract_jwt_from_request
from videbo.exceptions import FFMpegError, FFProbeError, MimeTypeNotAllowed, VideoNotAllowed
from videbo.misc import MEGA
from videbo.misc.functions import rel_path, run_in_default_executor
from videbo.models import RequestJWTData, Role, TokenIssuer
from videbo.network import NetworkInterfaces
from videbo.route_def import RouteTableDef
from videbo.temp_file import TempFile
from videbo.types import PathT
from videbo.video.models import VideoInfo
from videbo.video.analyze import generate_thumbnails, get_ffprobe_info, get_video_mime_type
from videbo.web import ensure_json_body, file_serve_headers, serve_file_via_x_accel
from videbo.storage.util import (FileStorage, StoredHashedVideoFile,
                                 is_allowed_file_ending, schedule_video_delete)
from videbo.storage.exceptions import (FileTooBigError, FormFieldMissing, BadFileExtension, UnknownDistURL,
                                       DistAlreadyDisabled, DistAlreadyEnabled)
from videbo.storage.distribution import DistributionNodeInfo
from .models import *


log = logging.getLogger(__name__)

routes = RouteTableDef()

EXTERNAL_JWT_LIFE_TIME = 3600  # 1 hour in seconds
CHUNK_SIZE_DEFAULT = 300 * 1024  # 300 KB in bytes

MULTIPART_FORM_DATA = "multipart/form-data"


@routes.get_with_cors('/api/upload/maxsize')
async def get_max_size(_: Request) -> Response:
    """Get max file size in MB."""
    return json_response({'max_size': settings.max_file_size_mb})


async def get_video_payload(request: Request) -> BodyPartReader:
    """
    Performs payload checks and extract the video file stream from the form.

    Assumes the request contains a multipart payload. Searches through it
    for a field named `video` and represented by a stream reader.

    Returns:
        `aiohttp.BodyPartReader` instance representing the video data stream

    Raises:
        `FormFieldMissing`:
            If the correct video form field is not found
        `BadFileExtension`:
            If the `filename` field does not have a whitelisted extension
        `FileTooBigError`:
            If the content length of the request is specified and
            exceeds the configured maximum allowed file size
    """
    multipart = await request.multipart()
    # TODO: Consider using an `async for`-loop with a `break` here instead
    field = await multipart.next()
    while not isinstance(field, BodyPartReader) or field.name != 'video':
        if field is None:
            raise FormFieldMissing()
        field = await multipart.next()
    if not is_allowed_file_ending(field.filename):
        log.warning(f"File extension not allowed: {field.filename}")
        raise BadFileExtension()
    if request.content_length and request.content_length > settings.max_file_size_mb * MEGA:
        raise FileTooBigError()
    return field


def invalid_format_response() -> Response:
    """Respond with 415 and error "invalid_format"."""
    log.warning("No or invalid file in request")
    return json_response({'error': 'invalid_format'}, status=415)


def file_too_big_response() -> Response:
    """Respond with 413 and "max_size" information."""
    log.warning("File too big to upload")
    return json_response({'max_size': settings.max_file_size_mb}, status=413)


async def read_data(
    temp_file: TempFile,
    video_form_field: BodyPartReader,
    chunk_size_bytes: int = CHUNK_SIZE_DEFAULT,
) -> None:
    """
    Writes video data into temp. file in chunks up to the configured limit.

    Args:
        temp_file:
            The `TempFile` instance to write the video data into
        video_form_field:
            The `aiohttp.BodyPartReader` object with the video data stream
        chunk_size_bytes (optional):
            The number of bytes to read asynchronously per chunk;
            defaults to `CHUNK_SIZE_DEFAULT`

    Raises:
        `FileTooBigError` if the configured maximum file size is exceeded
    """
    log.info("Start reading file from client")
    # TODO: Check if it is possible to use `async for`-loop here instead
    data = await video_form_field.read_chunk(chunk_size_bytes)
    while len(data) > 0:
        if temp_file.size > settings.max_file_size_mb * MEGA:
            raise FileTooBigError()
        await temp_file.write(data)
        data = await video_form_field.read_chunk(chunk_size_bytes)
    log.info(f"File was uploaded ({temp_file.size} bytes)")


async def get_video_info(path: PathT) -> VideoInfo:
    """
    Runs metadata checks on the video at `path` and returns the information.

    Logs failed checks as expressive warnings.

    Args:
        path: Path to the video file to analyze

    Returns:
        Instance of `VideoInfo` if the checks were passed

    Raises:
        `MimeTypeNotAllowed` if the video's MIME type is not whitelisted
        `FFProbeError` if analysis with `ffprobe` could not be completed
        `VideoNotAllowed` if the video's codec or format is not whitelisted
    """
    mime_type = await get_video_mime_type(path)
    if mime_type not in settings.video.mime_types_allowed:
        exception = MimeTypeNotAllowed(mime_type)
        log.warning(repr(exception))
        raise exception
    try:
        probe_info = await get_ffprobe_info(path)
        probe_info.ensure_is_allowed()
    except (FFProbeError, VideoNotAllowed) as e:
        log.warning(repr(e))
        raise
    return probe_info


async def save_temp_file_and_create_thumbnails(
    temp_file: TempFile,
    video_info: VideoInfo,
) -> FileUploadedResponseJWT:
    """
    Persists the temp. video file on disk and creates thumbnails alongside it.

    Logs file size, video duration, hash digest, and number of thumbnails.

    Args:
        temp_file:
            The `TempFile` instance to write the video data into
        video_info:
            The `VideoInfo` instance with the video meta information

    Returns:
        Valid `FileUploadedResponseJWT` instance
    """
    duration = round(video_info.get_duration(), 1)
    video_path = await temp_file.persist(file_ext=video_info.file_ext)
    size, hash_, ext = temp_file.size, temp_file.digest, video_path.suffix
    log.info(f"Temp video saved: {size=}, {duration=}, {hash_=}")
    thumb_count = await generate_thumbnails(
        video_path,
        duration,
        interim_dir=FileStorage.get_instance().temp_out_dir,
    )
    log.info(f"{thumb_count} thumbnails generated for video: {hash_}")
    return FileUploadedResponseJWT(
        exp=FileUploadedResponseJWT.default_expiration_from_now(),
        iss=TokenIssuer.external,
        hash=hash_,
        file_ext=ext,
        thumbnails_available=thumb_count,
        duration=duration
    )


def get_video_and_thumbnail_urls(
    file_hash: str,
    file_ext: str,
    *,
    temp: bool,
    thumb_count: int,
    exp: Optional[int] = None,
) -> tuple[str, list[str]]:
    """
    Constructs the URLs for requesting a video file and its thumbnails.

    Each URL is created via the `RequestFileJWTData.encode_public_file_url`.

    Args:
        file_hash:
            The hash hexdigest of the video file in question
        file_ext:
            The extension of the video file in question
        temp:
            If `True`, the video is assumed to not be saved permanently yet,
            but only recently uploaded.
        thumb_count:
            The number of thumbnails that can be requested for the video
        exp (optional):
            If provided, it will be set as the value for the `exp` field
            on the JWT model; otherwise the default expiration time is used.

    Returns:
        2-tuple with the first element being the URL by which to request
        the video and the second being a list of the thumbnail URLs
    """
    exp = exp or RequestFileJWTData.default_expiration_from_now()
    video_url = RequestFileJWTData.client_default(
        file_hash,
        file_ext,
        temp=temp,
        expiration_time=exp,
    ).encode_public_file_url()
    thumbnail_urls = []
    for thumb_id in range(thumb_count):
        thumbnail_urls.append(
            RequestFileJWTData.client_default(
                file_hash,
                file_ext,
                temp=temp,
                expiration_time=exp,
                thumb_id=thumb_id,
            ).encode_public_file_url()
        )
    return video_url, thumbnail_urls


async def save_temp_and_respond(
    temp_file: TempFile,
    video_form_field: BodyPartReader,
) -> Response:
    """
    Writes video data into temp. file, saves it and responds appropriately.

    If `FileTooBigError` is encountered while reading the data stream,
    the `file_too_big_response` is returned.
    If `FFProbeError` or `VideoNotAllowed` is raised during video analysis,
    the `invalid_format_response` is returned.
    In both those error cases the exception is _not_ propagated up the stack,
    but the temporary `file` is deleted.

    Args:
        temp_file:
            The `TempFile` instance to write the video data into
        video_form_field:
            The `aiohttp.BodyPartReader` object with the video data stream

    Returns:
        `aiohttp.web_response.Response` with a JSON body
    """
    try:
        async with temp_file.open():
            await read_data(temp_file, video_form_field)
    except FileTooBigError:
        await temp_file.delete()
        return file_too_big_response()
    try:
        video = await get_video_info(temp_file.path)
    except (FFProbeError, VideoNotAllowed):
        await temp_file.delete()
        return invalid_format_response()
    jwt_data = await save_temp_file_and_create_thumbnails(temp_file, video)
    video_url, thumbnail_urls = get_video_and_thumbnail_urls(
        jwt_data.hash,
        jwt_data.file_ext,
        temp=True,
        thumb_count=jwt_data.thumbnails_available,
    )
    data = {
        "result": "ok",
        "jwt": jwt_data.encode(),
        "url": video_url,
        "thumbnails": thumbnail_urls,
    }
    return json_response(data)


@routes.post_with_cors('/api/upload/file', allow_headers=['Authorization'])
@ensure_auth(Role.client)
async def upload_file(request: Request, jwt_token: UploadFileJWTData) -> Response:
    """User wants to upload a video."""
    if request.content_type != MULTIPART_FORM_DATA:
        raise HTTPNotAcceptable(headers={"Accept": MULTIPART_FORM_DATA})
    if not jwt_token.is_allowed_to_upload_file:
        raise HTTPForbidden()
    try:
        field = await get_video_payload(request)
    except (FormFieldMissing, BadFileExtension):
        return invalid_format_response()
    except FileTooBigError:
        return file_too_big_response()
    storage = FileStorage.get_instance()
    storage.num_current_uploads += 1
    file = TempFile.create(storage.temp_dir)
    # Any error should be followed by a cleanup of the temp. file
    try:
        return await save_temp_and_respond(file, field)
    except Exception as e:
        await file.delete()
        if isinstance(e, FFMpegError):
            log.error(repr(e))
        else:
            log.exception(e)
        raise HTTPInternalServerError()
    finally:
        storage.num_current_uploads -= 1


@routes.get('/api/save/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')
@ensure_auth(Role.lms)
async def save_file(request: Request, jwt_data: SaveFileJWTData) -> Response:
    """Confirms that the file should be saved permanently."""
    if not jwt_data.is_allowed_to_save_file:
        log.info("Unauthorized save request")
        raise HTTPForbidden()
    hash_, ext = request.match_info["hash"], request.match_info["file_ext"]
    try:
        await FileStorage.get_instance().store_permanently(hash_, ext)
    except FileNotFoundError:
        log.error(f"Save request failed; file(s) not found: {hash_}")
        return json_response(
            {"status": "error", "error": "file_does_not_exist"},
            status=404,
        )
    return json_response({"status": "ok"})


@routes.delete('/api/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')
@ensure_auth(Role.lms)
async def delete_file(request: Request, jwt_data: DeleteFileJWTData) -> Response:
    """Delete the file with the hash."""
    if not jwt_data.is_allowed_to_delete_file:
        log.info("unauthorized request")
        raise HTTPForbidden()

    origin = request.headers.getone("Origin", None)
    schedule_video_delete(request.match_info['hash'], request.match_info['file_ext'], origin=origin)
    return json_response({"status": "ok"})  # always succeeds


@routes.get('/file')
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
            await video_check_redirect(request, stored_file)
        log.debug(f"Serve video {hash_}")
    elif jwt_data.type == FileType.VIDEO_TEMP:
        path = file_storage.get_temp_video_path(hash_, ext)
        await verify_file_exists(path)  # no guarantee until the response!
        log.debug(f"Serve temp video {hash_}")
    else:  # must be FileType.THUMBNAIL or FileType.THUMBNAIL_TEMP
        return await handle_thumbnail_request(jwt_data)
    dl_name = request.query.get('downloadas')
    if not settings.webserver.x_accel_location:
        return FileResponse(path, headers=file_serve_headers(dl_name))
    uri = Path(settings.webserver.x_accel_location, rel_path(path.name))
    limit_rate = settings.webserver.get_x_accel_limit_rate(
        internal=jwt_data.iss == TokenIssuer.internal
    )
    return serve_file_via_x_accel(uri, limit_rate, download_filename=dl_name)


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
        if file.views >= settings.distribution.copy_views_threshold:
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
                        await async_sleep(1)
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
            await async_sleep(1)
            return video_redirect_to_node(request, node, file)
        else:
            return  # Serve file


def video_redirect_to_node(request: Request, node: DistributionNodeInfo, file: StoredHashedVideoFile) -> None:
    log.debug(f"Redirect user to {node.base_url} for video {file}")
    url = f"{node.base_url}/file?jwt={extract_jwt_from_request(request)}"
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
    if not await run_in_default_executor(path.is_file):
        log.warning(f"File does not exist: {path}")
        raise HTTPNotFound()


async def handle_thumbnail_request(jwt_data: RequestFileJWTData) -> Response:
    """
    Returns a regular `Response` with the previously cached thumbnail data.
    Uses the `FileStorage` instance's capabilities of storing recently requested thumbnails in RAM.
    The usual sanity checks apply.
    """
    if jwt_data.thumb_id is None:
        log.warning("JWT missing `thumb_id`")
        raise HTTPBadRequest()
    hash_, ext, num = jwt_data.hash, jwt_data.file_ext, jwt_data.thumb_id
    file_storage = FileStorage.get_instance()
    if jwt_data.type == FileType.THUMBNAIL:
        try:
            path = file_storage.get_perm_thumbnail_path(hash_, ext, num=num)
        except FileNotFoundError:
            raise HTTPNotFound()
        log.debug(f"Serve thumbnail {num} for video {hash_}")
    elif jwt_data.type == FileType.THUMBNAIL_TEMP:
        path = file_storage.get_temp_thumbnail_path(hash_, num=num)
        log.debug(f"Serve temp thumbnail {num} for temp video {hash_}")
    else:
        raise RuntimeError("invalid request type")  # should never happen
    body: Optional[bytes] = file_storage.thumb_memory_cache.get(path)
    if body is None:
        try:
            body = await run_in_default_executor(path.read_bytes)
        except FileNotFoundError:
            log.warning(f"File does not exist: {path}")
            raise HTTPNotFound()
        assert isinstance(body, bytes)
        # Setting cache maximum size to 0 effectively disables it:
        if settings.thumbnails.cache_max_mb > 0:
            file_storage.thumb_memory_cache[path] = body
    return Response(
        body=body,
        content_type="image/jpeg",
        headers=file_serve_headers(),
    )


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


@routes.post(r'/api/storage/distributor/disable')
@ensure_auth(Role.admin)
@ensure_json_body
async def disable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    await set_dist_node_state(data.base_url, enabled=False)
    raise HTTPOk()


@routes.post(r'/api/storage/distributor/enable')
@ensure_auth(Role.admin)
@ensure_json_body
async def enable_dist_node(_request: Request, _jwt_data: RequestJWTData, data: DistributorNodeInfo) -> NoReturn:
    await set_dist_node_state(data.base_url, enabled=True)
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


@routes.post('/api/storage/delete')
@ensure_auth(Role.admin)
@ensure_json_body
async def batch_delete_files(_request: Request, _jwt_data: RequestJWTData, data: DeleteFilesList) -> Response:
    storage = FileStorage.get_instance()
    not_deleted = list(await storage.remove_files(*data.hashes))
    if not_deleted:
        return json_response({'status': 'incomplete', 'not_deleted': not_deleted})
    return json_response({'status': 'ok'})
