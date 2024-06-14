from asyncio import sleep as async_sleep
from logging import Logger, getLogger
from pathlib import Path

from aiohttp.multipart import BodyPartReader
from aiohttp.web_exceptions import (
    HTTPBadRequest,
    HTTPNotFound,
    HTTPServiceUnavailable,
)
from aiohttp.web_request import Request
from aiohttp.web_response import Response

from videbo import settings
from videbo.distributor.api.redirect import RedirectToDistributor
from videbo.exceptions import FFProbeError, VideoNotAllowed
from videbo.misc.functions import run_in_default_executor
from videbo.network import NetworkInterfaces
from videbo.temp_file import TempFile
from videbo.video.analyze import generate_thumbnails, get_video_info
from videbo.web import file_serve_headers
from .api.models import (
    FileTooBig,
    FileType,
    FileUploaded,
    InvalidFormat,
    RequestFileJWTData,
)
from .exceptions import (
    BadFileExtension,
    FormFieldMissing,
    FileTooBigError,
)
from .stored_file import StoredVideoFile
from .file_controller import StorageFileController, is_allowed_file_ending

__all__ = [
    "CHUNK_SIZE_DEFAULT",
    "get_video_payload",
    "save_temp_and_get_response",
    "handle_video_request",
    "verify_file_exists",
    "handle_thumbnail_request",
]

_log = getLogger(__name__)

CHUNK_SIZE_DEFAULT = 300 * 1024  # 300 KB in bytes


async def get_video_payload(
    request: Request,
    log: Logger = _log,
) -> BodyPartReader:
    """
    Performs payload checks and extract the video file stream from the form.

    Assumes the request contains a multipart payload. Searches through it
    for a field named `video` and represented by a stream reader.

    Args:
        request: The `aiohttp.web_request.Request` instance
        log (optional): Logger instance to use (defaults to the module log)

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
    field = await multipart.next()
    while not isinstance(field, BodyPartReader) or field.name != 'video':
        if field is None:
            raise FormFieldMissing()
        field = await multipart.next()
    if not is_allowed_file_ending(field.filename):
        log.warning(f"File extension not allowed: {field.filename}")
        raise BadFileExtension()
    if request.content_length and request.content_length > settings.video.max_file_size_bytes:
        raise FileTooBigError()
    return field


async def read_data(
    temp_file: TempFile,
    video_form_field: BodyPartReader,
    chunk_size_bytes: int = CHUNK_SIZE_DEFAULT,
    log: Logger = _log,
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
        log (optional):
            Logger instance to use (defaults to the module log)

    Raises:
        `FileTooBigError` if the configured maximum file size is exceeded
    """
    log.info("Start reading file from client")
    data = await video_form_field.read_chunk(chunk_size_bytes)
    while len(data) > 0:
        if temp_file.size + len(data) > settings.video.max_file_size_bytes:
            raise FileTooBigError()
        await temp_file.write(data)
        data = await video_form_field.read_chunk(chunk_size_bytes)
    log.info(f"File was uploaded ({temp_file.size} bytes)")


async def save_temp_video(
    temp_file: TempFile,
    video_form_field: BodyPartReader,
    log: Logger = _log,
) -> float:
    async with temp_file.open():
        await read_data(temp_file, video_form_field, log=log)
    video_info = await get_video_info(temp_file.path, log=log)
    await temp_file.persist(file_ext=video_info.get_consistent_file_ext())
    return round(video_info.get_duration(), 1)


async def save_temp_and_get_response(
    temp_file: TempFile,
    video_form_field: BodyPartReader,
    log: Logger = _log,
) -> Response:
    """
    Writes video data into temp. file, saves it and responds appropriately.

    If `FileTooBigError` is encountered while reading the data stream,
    the `FileTooBig` response is returned.
    If `FFProbeError` or `VideoNotAllowed` is raised during video analysis,
    the `InvalidFormat` response is returned.
    In both those error cases the exception is _not_ propagated up the stack,
    but the `temp_file` is deleted.

    Args:
        temp_file:
            The `TempFile` instance to write the video data into
        video_form_field:
            The `aiohttp.BodyPartReader` object with the video data stream
        log (optional):
            Logger instance to use (defaults to the module log)

    Returns:
        `aiohttp.web_response.Response` with a JSON body
    """
    try:
        duration = await save_temp_video(temp_file, video_form_field, log)
    except FileTooBigError:
        await temp_file.delete()
        return FileTooBig().json_response(log=log)
    except (FFProbeError, VideoNotAllowed):
        await temp_file.delete()
        return InvalidFormat().json_response(log=log)
    size, digest = temp_file.size, temp_file.digest
    log.info(f"Temp video saved: {size=}, {duration=}, {digest=}")
    thumb_count = await generate_thumbnails(
        temp_file.path,
        duration,
        interim_dir=StorageFileController.get_instance().temp_out_dir,
    )
    log.info(f"{thumb_count} thumbnails generated for video: {digest}")
    data = FileUploaded.from_video(
        digest,
        temp_file.path.suffix,
        thumbnails_available=thumb_count,
        duration=duration,
    )
    return data.json_response()


def ensure_acceptable_load(
    tx_load: float,
    tx_load_threshold: float = settings.max_load_file_serving,
    log: Logger = _log,
) -> None:
    """Raises `HTTPServiceUnavailable` (503) if load exceeds threshold."""
    if tx_load > tx_load_threshold:
        log.warning(
            "Cannot serve video; "
            f"TX load {tx_load:.1%} above threshold {tx_load_threshold:.1%}"
        )
        raise HTTPServiceUnavailable()


async def handle_video_request(
    request: Request,
    file: StoredVideoFile,
    log: Logger = _log,
) -> None:
    """
    Handles possible `file` distribution and `request` redirection.

    Tries to find a distributor that can serve the file and redirects to it.

    If no redirect can happen and the storage node should serve the file,
    this function simply returns `None`.

    Otherwise either an error is raised, if all nodes (including the storage
    node) are too busy, or a redirect 302 to a distributor is issued via
    `RedirectToDistributor`.

    Distribution (i.e. copying the file to another node) may be initialized,
    regardless of whether an available distributor node is found.

    If the HTTP `Range` header is present and the start value is not zero,
    a redirect will never occur.

    Args:
        request:
            The `aiohttp.web_request.Request` instance
        file:
            The `StoredVideoFile` object representing the file
        log (optional):
            Logger instance to use (defaults to the module log)

    Raises:
        `RedirectToDistributor` (302)
            if redirection to a distributor node should occur
        `HTTPServiceUnavailable` (503)
            if all distributors and the storage node are too busy
    """
    tx_load = NetworkInterfaces.get_instance().get_tx_load() or 0.
    dist_controller = StorageFileController.get_instance().distribution_controller
    dist_controller.handle_distribution(file, tx_load)
    if request.http_range.start is not None and request.http_range.start > 0:
        # Never redirect requests for later parts of the video
        ensure_acceptable_load(tx_load, log=log)
        return  # Serve file
    node, has_complete_file = dist_controller.get_node_to_serve(file)
    if node is None:
        # Found no distribution node that has the file or is downloading it
        ensure_acceptable_load(tx_load, log=log)
        return  # Serve file
    if has_complete_file:
        # Found distribution node that can serve the file immediately
        raise RedirectToDistributor(request, node, file, log=log)
    # Only found a distribution that is currently downloading the file
    if tx_load <= settings.distribution.load_threshold_delayed_redirect:
        return  # Serve file
    # Storage is fairly busy; we wait a moment to give the distributor
    # enough time to process the request to copy the file to it;
    # then we redirect to that node, knowing that the client will have
    # to wait until the file was fully copied to the node
    await async_sleep(1)
    raise RedirectToDistributor(request, node, file, log=log)


async def verify_file_exists(path: Path, log: Logger = _log) -> None:
    if not await run_in_default_executor(path.is_file):
        log.warning(f"File does not exist: {path}")
        raise HTTPNotFound()


async def handle_thumbnail_request(
    jwt_data: RequestFileJWTData,
    log: Logger = _log,
) -> Response:
    """
    Returns a regular `Response` with the raw thumbnail data.

    Uses the `StorageFileController` capabilities of caching recently requested
    thumbnails in RAM, if the the cache option was configured accordingly.

    Args:
        jwt_data:
            Decoded `RequestFileJWTData` from the token required for download
        log (optional):
            Logger instance to use (defaults to the module log)

    Returns:
        A `aiohttp.web_response.Response` with the `image/jpeg` content type
        and the thumbnail file as the response body

    Raises:
        `HTTPBadRequest` (400)
            if the JWT field `thumb_id` is `None`
        `HTTPNotFound` (404)
            if the requested thumbnail was not found in storage
    """
    if jwt_data.thumb_id is None:
        log.warning("JWT missing `thumb_id`")
        raise HTTPBadRequest()
    # TODO: Consider checking file serving load threshold here too
    hash_, ext, num = jwt_data.hash, jwt_data.file_ext, jwt_data.thumb_id
    file_storage = StorageFileController.get_instance()
    if jwt_data.type == FileType.THUMBNAIL:
        try:
            path = file_storage.get_perm_thumbnail_path(hash_, ext, num=num)
        except FileNotFoundError:
            raise HTTPNotFound() from None
        log.debug(f"Serve thumbnail {num} for video {hash_}")
    elif jwt_data.type == FileType.THUMBNAIL_TEMP:
        path = file_storage.get_temp_thumbnail_path(hash_, num=num)
        log.debug(f"Serve temp thumbnail {num} for temp video {hash_}")
    else:
        raise RuntimeError("invalid request type")  # should be unreachable
    try:
        body = await file_storage.thumb_memory_cache.get_and_update(path)
    except FileNotFoundError:
        log.error(f"File does not exist: {path}")
        raise HTTPNotFound() from None
    return Response(
        body=body,
        content_type="image/jpeg",
        headers=file_serve_headers(),
    )
