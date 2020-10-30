from typing import List
import time
import asyncio
import logging
import urllib.parse
from distutils.util import strtobool

from aiohttp import BodyPartReader
from aiohttp.web import Request, Response
from aiohttp.web import FileResponse
from aiohttp.web import json_response
from aiohttp.web import RouteTableDef
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_exceptions import HTTPForbidden
from aiohttp.web_exceptions import HTTPNotFound
from aiohttp.web_exceptions import HTTPNotAcceptable
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPFound, HTTPServiceUnavailable

from videbo.web import ensure_json_body, register_route_with_cors, json_response as model_json_response
from videbo.web import ensure_no_reverse_proxy
from videbo.auth import external_jwt_encode
from videbo.auth import ensure_jwt_data_and_role
from videbo.auth import Role, JWT_ISS_INTERNAL
from videbo.auth import BaseJWTData
from videbo.misc import get_free_disk_space
from videbo.misc import sanitize_filename
from videbo.network import NetworkInterfaces
from videbo.video import VideoInfo
from videbo.video import VideoValidator
from videbo.video import VideoConfig

from videbo.storage.api.models import UploadFileJWTData
from videbo.storage.api.models import SaveFileJWTData
from videbo.storage.api.models import DeleteFileJWTData
from videbo.storage.api.models import RequestFileJWTData
from videbo.storage.api.models import FileType, StorageStatus, DistributorNodeInfo
from videbo.storage.api.models import StorageFileInfo, StorageFilesList, DeleteFilesList
from videbo.storage.distribution import DistributionNodeInfo
from videbo.storage.util import TempFile
from videbo.storage.util import FileStorage
from videbo.storage.exceptions import NoValidFileInRequestError
from videbo.storage.exceptions import FileTooBigError
from videbo.storage.util import HashedVideoFile, StoredHashedVideoFile
from videbo.storage.util import is_allowed_file_ending, schedule_video_delete
from videbo.exceptions import InvalidMimeTypeError, InvalidVideoError, FFProbeError

from videbo.storage import storage_logger
from videbo.storage import storage_settings
routes = RouteTableDef()


access_logger = logging.getLogger('videbo-storage-access')
EXTERNAL_JWT_LIFE_TIME = 3600


def generate_video_url(video: HashedVideoFile, temp: bool) -> str:
    data = {
        "role": "client",
        "type": 'video_temp' if temp else 'video',
        "hash": video.hash,
        "file_ext": video.file_extension,
        "rid": "",
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
            "file_ext": video.file_extension,
            "rid": "",
        }
        jwt_data = external_jwt_encode(data, EXTERNAL_JWT_LIFE_TIME)
        urls.append(f"{storage_settings.public_base_url}/file?jwt={jwt_data}")

    return urls


async def read_data(request: Request) -> TempFile:
    """Read the video file and return the temporary file with the uploaded data."""
    max_file_size = storage_settings.max_file_size_mb * 1024 * 1024
    multipart = await request.multipart()

    # Skip to the field that we are interested in.
    field = await multipart.next()
    while not isinstance(field, BodyPartReader) or field.name != "video":
        if field is None:
            raise NoValidFileInRequestError()
        field = await multipart.next()

    # First simple file type check.
    if not is_allowed_file_ending(field.filename):
        storage_logger.warning(f"file ending not allowed ({field.filename})")
        raise NoValidFileInRequestError()

    # Check file/content size as reported in the header if supplied.
    if request.content_length and request.content_length > max_file_size:
        raise FileTooBigError()

    # Create temp file and read data from client.
    storage_logger.info("Start reading file from client")
    file: TempFile = FileStorage.get_instance().create_temp_file()

    chunk_size = 300 * 1024  # Bytes
    data = await field.read_chunk(chunk_size)
    while len(data) > 0:
        if file.size > max_file_size:
            await file.delete()
            raise FileTooBigError()
        await file.write(data)
        data = await field.read_chunk(chunk_size)

    await file.close()
    storage_logger.info(f"File was uploaded ({file.size} Bytes)")
    return file


@register_route_with_cors(routes, 'GET', '/api/upload/maxsize')
async def get_max_size(_: Request):
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
        video = VideoInfo(video_file=file.path, video_config=VideoConfig(storage_settings))
        validator = VideoValidator(info=video)
        await video.fetch_mime_type()
        validator.check_valid_mime_type()
        await video.fetch_info() 
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
    except FileNotFoundError:
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

    origin = request.headers.getone("Origin", None)
    schedule_video_delete(request.match_info['hash'], request.match_info['file_ext'], origin)
    return json_response({"status": "ok"})  # always succeeds


@routes.get('/file')
@ensure_jwt_data_and_role(Role.client)
async def request_file(request: Request, jwt_data: RequestFileJWTData):
    """Serve a video or a thumbnail."""

    # Check all required data.
    video = HashedVideoFile(jwt_data.hash, jwt_data.file_ext)
    file_storage = FileStorage.get_instance()

    if jwt_data.type == FileType.VIDEO:
        # Record the video access and find out if this node serves the file or should redirect to a distributor node.
        try:
            video = await file_storage.get_file(jwt_data.hash, jwt_data.file_ext)
        except FileNotFoundError:
            raise HTTPNotFound()

        # Only consider redirecting the client when it is an external request.
        if jwt_data.iss != JWT_ISS_INTERNAL:
            file_storage.distribution_controller.count_file_access(video, jwt_data.rid)
            await video_check_redirect(request, video, jwt_data.rid)

        access_logger.info(f"serve video with hash {video}")
        path = file_storage.get_path(video)

    elif jwt_data.type == FileType.VIDEO_TEMP:
        access_logger.info(f"serve temp video with hash {video}")
        path = file_storage.get_path_in_temp(video)

    elif jwt_data.type == FileType.THUMBNAIL or jwt_data.type == FileType.THUMBNAIL_TEMP:
        if jwt_data.thumb_id is None:
            storage_logger.info('thumb ID is None in JWT')
            raise HTTPBadRequest()

        if jwt_data.type == FileType.THUMBNAIL:
            access_logger.info(f"serve thumbnail {jwt_data.thumb_id} for video with hash {video}")
            path = file_storage.get_thumb_path(video, jwt_data.thumb_id)

        else:
            access_logger.info(f"serve temp thumbnail {jwt_data.thumb_id} for video with hash {video}")
            path = file_storage.get_thumb_path_in_temp(video, jwt_data.thumb_id)
    else:
        storage_logger.info(f"unknown request type: {jwt_data.type}")
        raise HTTPBadRequest()

    # Check if the file really exists. If a video file is requested we already know the file should exist.
    if jwt_data.type != FileType.VIDEO and not (await asyncio.get_event_loop().run_in_executor(None, path.is_file)):
        storage_logger.warn(f"file does not exist: {path}")
        raise HTTPNotFound()

    headers = {
        "Cache-Control": "private, max-age=50400"
    }

    if "downloadas" in request.query:
        filename = sanitize_filename(request.query["downloadas"])
        filename = urllib.parse.quote(filename)
        headers["Content-Disposition"] = f'attachment; filename="{filename}"'

    return FileResponse(path, headers=headers)


async def video_check_redirect(request: Request, file: StoredHashedVideoFile, rid: str) -> None:
    own_tx_load = get_own_tx_load()
    node, has_complete_file = file.nodes.find_good_node(file)
    if node is None:
        # There is no distribution node.
        if file.views >= storage_settings.copy_to_dist_views_threshold:
            if file.nodes.copying:
                # When we are here this means that there is no non-busy distribution node. Even the dist node that
                # is currently loading the file is too busy.
                storage_logger.info(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f} "
                                    f"and waiting for copying to complete")
                raise HTTPServiceUnavailable()
            else:
                to_node = FileStorage.get_instance().distribution_controller.copy_file_to_one_node(file)
                if to_node is None:
                    if own_tx_load > 0.9:
                        # There is no dist node to copy the file to and this storage node is too busy.
                        storage_logger.warning(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f}")
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
                        file.init_dist_by(rid)
                        return  # Serve file
        else:
            if own_tx_load > 0.9:
                # The file is not requested that often and this storage node is too busy.
                storage_logger.warning(f"Cannot serve video, node too busy (tx load {own_tx_load:.2f}")
                raise HTTPServiceUnavailable()
            else:
                # This storage node is not too busy and can serve the file by itself.
                return
    elif file.prevent_redirect(rid):
        return  # Serve file
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


def video_redirect_to_node(request: Request, node: DistributionNodeInfo, file: StoredHashedVideoFile):
    access_logger.info(f"Redirect user to {node.base_url} for video {file}")
    jwt = request.query['jwt']
    url = f"{node.base_url}/file?jwt={jwt}"
    downloadas = request.query.getone("downloadas", None)
    if downloadas:
        url += "&downloadas=" + urllib.parse.quote(downloadas)
    raise HTTPFound(url)


def get_own_tx_load() -> float:
    network = NetworkInterfaces.get_instance()
    interfaces = network.get_interface_names()
    if len(interfaces) == 0:
        return 0
    iface = network.get_interface(interfaces[0])
    return (iface.tx_throughput * 8 / 1_000_000) / storage_settings.tx_max_rate_mbit


@routes.post(r'/api/storage/distributor/add')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def add_dist_node(_request: Request, _jwt_data: BaseJWTData, data: DistributorNodeInfo):
    await FileStorage.get_instance().distribution_controller.add_new_dist_node(data.base_url)
    return Response()


@routes.post(r'/api/storage/distributor/remove')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def remove_dist_node(_request: Request, _jwt_data: BaseJWTData, data: DistributorNodeInfo):
    await FileStorage.get_instance().distribution_controller.remove_dist_node(data.base_url)
    return Response()


@routes.get(r'/api/storage/status')
@ensure_jwt_data_and_role(Role.manager)
async def get_status(_request: Request, _jwt_data: BaseJWTData):
    status = StorageStatus.construct()
    storage = FileStorage.get_instance()

    status.files_total_size = storage.get_files_total_size_mb()
    status.files_count = storage.get_files_count()
    status.free_space = await get_free_disk_space(str(storage_settings.videos_path))

    status.tx_max_rate = storage_settings.tx_max_rate_mbit
    network = NetworkInterfaces.get_instance()
    interfaces = network.get_interface_names()

    if storage_settings.server_status_page:
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
        storage_logger.error("No network interface found!")

    status.distributor_nodes = storage.distribution_controller.get_dist_node_base_urls()

    return model_json_response(status)


@routes.get(r'/api/storage/files')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
async def get_files_list(request: Request, _jwt_data: BaseJWTData) -> Response:
    files = []
    storage = FileStorage.get_instance()
    if request.query:
        orphaned = request.query.get('orphaned')
        if orphaned:
            orphaned = bool(strtobool(orphaned.lower()))
        files_dict = await storage.filtered_files(orphaned=orphaned)
    else:
        files_dict = storage.all_files()
    for file in files_dict.values():
        files.append(StorageFileInfo(hash=file.hash, file_extension=file.file_extension, file_size=file.file_size))
    return model_json_response(StorageFilesList(files=files))


@routes.post('/api/storage/delete')
@ensure_no_reverse_proxy
@ensure_jwt_data_and_role(Role.admin)
@ensure_json_body()
async def batch_delete_files(_request: Request, _jwt_data: BaseJWTData, data: DeleteFilesList) -> Response:
    storage = FileStorage.get_instance()
    removed_list = await storage.remove_files(*data.hashes)
    results = zip(data.hashes, removed_list)
    not_deleted = [file_hash for file_hash, success in results if not success]
    if not_deleted:
        return json_response({'status': 'incomplete', 'not_deleted': not_deleted})
    return json_response({'status': 'ok'})
