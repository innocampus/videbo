import asyncio
import pathlib
from aiohttp import BodyPartReader
from aiohttp.web import Request
from aiohttp.web import FileResponse
from aiohttp.web import json_response
from aiohttp.web import RouteTableDef
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPForbidden
from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_exceptions import HTTPNotAcceptable
from aiohttp.web_exceptions import HTTPUnsupportedMediaType
from aiohttp.web_exceptions import HTTPRequestEntityTooLarge
from aiohttp.web_exceptions import HTTPInternalServerError
from typing import List

from livestreaming.web import register_route_with_cors
from livestreaming.auth import external_jwt_encode
from livestreaming.auth import ensure_jwt_data_and_role
from livestreaming.auth import Role
from livestreaming.auth import BaseJWTData
from livestreaming.storage.api.models import UploadFileJWTData
from livestreaming.storage.api.models import SaveFileJWTData
from livestreaming.storage.api.models import DeleteFileJWTData
from livestreaming.storage.api.models import RequestFileJWTData
from livestreaming.storage.api.models import FileType
from livestreaming.storage.util import TempFile
from livestreaming.storage.util import FileStorage
from livestreaming.storage.util import FileDoesNotExistError
from livestreaming.storage.util import NoValidFileInRequestError
from livestreaming.storage.util import FileTooBigError
from livestreaming.storage.util import HashedVideoFile
from livestreaming.storage.util import FFProbeError
from livestreaming.storage.util import is_allowed_file_ending
from livestreaming.storage.video import VideoInfo
from livestreaming.storage.video import VideoValidator
from livestreaming.storage.exceptions import InvalidMimeTypeError
from livestreaming.storage.exceptions import InvalidVideoError

from livestreaming.storage import storage_logger
from livestreaming.storage import storage_settings
routes = RouteTableDef()

EXTERNAL_JWT_LIFE_TIME = 3600


def generate_video_url(video: HashedVideoFile, temp: bool) -> str:
    data = {
        "role": "client",
        "type": 'video_temp' if temp else 'video',
        "hash": video.hash,
        "file_ext": video.file_extension
    }
    jwt_data = external_jwt_encode(data, EXTERNAL_JWT_LIFE_TIME)
    return f"{storage_settings.public_base_url}/file?jwt={jwt_data}"


def generate_thumb_urls(video: HashedVideoFile, temp: bool, thumb_count: int) -> List[str]:
    urls = []
    for thumb_id in range(thumb_count):
        data = {
            "role": "client",
            "type": "thumbnail_temp" if temp else "thumbnail",
            "hash": video.hash,
            "thumb_id": thumb_id,
            "file_ext": video.file_extension
        }
        jwt_data = external_jwt_encode(data, EXTERNAL_JWT_LIFE_TIME)
        urls.append(f"{storage_settings.public_base_url}/file?jwt={jwt_data}")

    return urls


async def read_data(request: Request) -> TempFile:
    """Read the video file and return the temporary file with the uploaded data."""
    max_file_size = storage_settings.max_file_size_mb * 1024 * 1024
    multipart = await request.multipart()

    # Skip to the field that we are interested in.
    while True:
        field = await multipart.next()
        if isinstance(field, BodyPartReader) and field.name == "video":
            break

        if field is None:
            raise NoValidFileInRequestError()

    # First simple file type check.
    if not is_allowed_file_ending(field.filename):
        storage_logger.warning(f"file ending not allowed ({field.filename})")
        raise NoValidFileInRequestError()

    # Check file/content size as reported in the header if supplied.
    if request.content_length and request.content_length > max_file_size:
        raise FileTooBigError()

    # Create temp file and read data from client.
    storage_logger.info("Start reading file from client")
    file = FileStorage.get_instance().create_temp_file()  # type: TempFile
    while True:
        if file.size > max_file_size:
            await file.delete()
            raise FileTooBigError()

        data = await field.read_chunk(50 * 1024)
        if len(data) == 0:
            # eof reached
            break

        await file.write(data)

    await file.close()
    return file


@register_route_with_cors(routes, 'GET', '/api/upload/maxsize')
async def get_max_size(_: Request ):
    """Get max file size in mb."""
    return json_response({'max_size': storage_settings.max_file_size_mb})


@register_route_with_cors(routes, "POST", "/api/upload/file", ["Authorization"])
@ensure_jwt_data_and_role(Role.client)
async def upload_file(request: Request, jwt_token: UploadFileJWTData):
    """User wants to upload a video."""

    if request.content_type != "multipart/form-data":
        raise HTTPNotAcceptable(headers={"Accept": "multipart/form-data"})

    # check JWT permission
    if not jwt_token.is_allowed_to_upload_file:
        raise HTTPForbidden()

    try:
        file = await read_data(request)
    except NoValidFileInRequestError:
        storage_logger.warning("no or invalid file found in request.")
        return json_response({'error': 'invalid_format'}, status=415)
    except FileTooBigError:
        storage_logger.warning("client wanted to upload file that is too big.")
        return json_response({'max_size': storage_settings.max_file_size_mb}, status=413)
    except NotADirectoryError:
        raise HTTPInternalServerError()

    try:
        video = VideoInfo(video_file=file.path)
        validator = VideoValidator(info=video)
        await video.fetch_mime_type(binary=storage_settings.binary_file, user=storage_settings.check_user)
        validator.check_valid_mime_type()
        await video.fetch_info(binary=storage_settings.binary_ffprobe, user=storage_settings.check_user)
        validator.check_valid_video()

        video_duration = int(video.get_length())
        file_ext = video.get_suggested_file_extension()
        stored_file = await file.persist(file_ext)

        file_storage = FileStorage.get_instance()
        video.video_file = file_storage.get_path_in_temp(stored_file)
        storage_logger.info(f"saved temp file, size: {file.size}, duration: {video_duration}, hash: {stored_file.hash}")
        thumb_count = await file_storage.generate_thumbs(stored_file, video)
        storage_logger.info(f"saved {thumb_count} temp thumbnails for temp video, hash: {stored_file.hash}")

        jwt_data = {
            "hash": stored_file.hash,
            "file_ext": stored_file.file_extension,
            "thumbnails_available": thumb_count,
            "duration": video_duration,
        }

        resp = {
            "result": "ok",
            "jwt": external_jwt_encode(jwt_data, EXTERNAL_JWT_LIFE_TIME),
            "url": generate_video_url(stored_file, True),
            "thumbnails": generate_thumb_urls(stored_file, True, thumb_count),
        }
        return json_response(resp)

    except FFProbeError as ffprobe_err:
        storage_logger.warning("invalid video file found in request (ffprobe error, timeout=%i, stderr below).",
                               ffprobe_err.timeout)
        if ffprobe_err.stderr is not None:
            storage_logger.warn(ffprobe_err.stderr)
        await file.delete()
        return json_response({'error': 'invalid_format'}, status=415)
    except InvalidVideoError as video_err:
        storage_logger.warning("invalid video file found in request (video: %s, audio: %s, container: %s).",
                               video_err.video_codec, video_err.audio_codec, video_err.container)
        await file.delete()
        return json_response({'error': 'invalid_format'}, status=415)
    except InvalidMimeTypeError as mimetype_err:
        storage_logger.warning(f"invalid video mime type found in request (mime type: {mimetype_err.mime_type}).")
        await file.delete()
        return json_response({'error': 'invalid_format'}, status=415)
    except Exception as err:
        await file.delete()
        storage_logger.exception(err)
        raise HTTPInternalServerError()


@routes.get('/api/save/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')
@ensure_jwt_data_and_role(Role.lms)
async def save_file(request: Request, jwt_data: SaveFileJWTData):
    """Confirms that the file should be saved permanently."""
    if not jwt_data.is_allowed_to_save_file:
        storage_logger.info('unauthorized request')
        raise HTTPForbidden()

    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])
    try:
        file_storage = FileStorage.get_instance()
        await file_storage.add_file_from_temp(file)
        thumb_count = storage_settings.thumb_suggestion_count
        await file_storage.add_thumbs_from_temp(file, thumb_count)
    except FileDoesNotExistError:
        storage_logger.error('Cannot save file with hash %s to video, file does not exist.', file.hash)
        return json_response({'status': 'error', 'error': 'file_does_not_exist'})

    return json_response({'status': 'ok'})


@routes.delete('/api/file/{hash:[0-9a-f]{64}}{file_ext:\\.[0-9a-z]{1,10}}')
@ensure_jwt_data_and_role(Role.lms)
async def delete_file(request: Request, jwt_data: DeleteFileJWTData):
    """Delete the file with the hash."""
    if not jwt_data.is_allowed_to_delete_file:
        storage_logger.info("unauthorized request")
        raise HTTPForbidden()

    file = HashedVideoFile(request.match_info['hash'], request.match_info['file_ext'])

    await FileStorage.get_instance().remove_thumbs(file)

    try:
        await FileStorage.get_instance().remove(file)
    except FileDoesNotExistError:
        storage_logger.error('Cannot delete file with hash %s from video, file does not exist.', file.hash)
        return json_response({'status': 'error', 'error': 'file_does_not_exist'})

    return json_response({"status": "ok"})


@routes.get('/file')
@ensure_jwt_data_and_role(Role.client)
async def request_file(_: Request, jwt_data: RequestFileJWTData):
    """Serve a video or a thumbnail."""

    # Check all required data.
    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    file_storage = FileStorage.get_instance()

    if jwt_data.type == FileType.STORAGE:
        storage_logger.info(f"serve storage with hash {video}")
        path = file_storage.get_path(video)[0]

    elif jwt_data.type == FileType.VIDEO_TEMP:
        storage_logger.info(f"serve temp storage with hash {video}")
        path = file_storage.get_path_in_temp(video)

    elif jwt_data.type == FileType.THUMBNAIL or jwt_data.type == FileType.THUMBNAIL_TEMP:
        if jwt_data.thumb_id is None:
            storage_logger.info('thumb ID is None in JWT')
            raise HTTPBadRequest()

        if jwt_data.type == FileType.THUMBNAIL:
            storage_logger.info(f"serve thumbnail {jwt_data.thumb_id} for video with hash {video}")
            path = file_storage.get_thumb_path(video, jwt_data.thumb_id)[0]

        else:
            storage_logger.info(f"serve temp thumbnail {jwt_data.thumb_id} for video with hash {video}")
            path = file_storage.get_thumb_path_in_temp(video, jwt_data.thumb_id)
    else:
        storage_logger.info(f"unknown request type: {jwt_data.type}")
        raise HTTPBadRequest()

    path = pathlib.Path(path)
    if not (await asyncio.get_event_loop().run_in_executor(None, path.is_file)):
        storage_logger.error(f"file does not exist: {path}")
        raise HTTPNotFound()

    return FileResponse(path, headers={"Cache-Control": "private, max-age=50400"})
