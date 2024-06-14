import asyncio
import logging
from pathlib import Path
from typing import NoReturn, Union

from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound, HTTPOk, HTTPServiceUnavailable, HTTPInternalServerError
from aiohttp.web_response import Response
from aiohttp.web_fileresponse import FileResponse

from videbo import settings
from videbo.auth import ensure_auth
from videbo.misc.functions import rel_path
from videbo.models import HashedFilesList, Role, RequestJWTData
from videbo.web import ensure_json_body, file_serve_headers, serve_file_via_x_accel
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.distributor.exceptions import (
    NotSafeToDelete,
    NoSuchFile,
    TooManyWaitingClients,
)
from videbo.distributor.files import DistributorFileController
from .models import (
    DistributorCopyFile,
    DistributorDeleteFiles,
    DistributorDeleteFilesResponse,
    DistributorFileList,
)


log = logging.getLogger(__name__)

routes = RouteTableDef()


@routes.get(r'/api/distributor/status')
@ensure_auth(Role.node)
async def get_status(_request: Request, _jwt_data: RequestJWTData) -> Response:
    """
    Returns the node's status information.

    Args:
        _request:
            Not considered (required for the route decorator)
        _jwt_data:
            Not considered (required for the auth. decorator)

    Returns:
        A `Response` built from the `DistributorStatus` model.
    """
    dist_status = await DistributorFileController.get_instance().get_status()
    return dist_status.json_response()


@routes.get(r'/api/distributor/files')
@ensure_auth(Role.node)
async def get_all_files(_request: Request, _jwt_data: RequestJWTData) -> Response:
    """
    Returns a list of all files controlled by the node.

    Args:
        _request:
            Not considered (required for the route decorator)
        _jwt_data:
            Not considered (required for the auth. decorator)

    Returns:
        A `Response` built from the `DistributorFileList` model.
    """
    return DistributorFileList.parse_obj({
        "files": list(DistributorFileController.get_instance().iter_files())
    }).json_response()


@routes.post(r'/api/distributor/copy/{hash:[0-9a-f]{64}}{file_ext:\.[0-9a-z]{1,10}}')
@ensure_auth(Role.node)
@ensure_json_body
async def copy_file(
    request: Request,
    _jwt_data: RequestJWTData,
    data: DistributorCopyFile,
) -> NoReturn:
    """
    Instructs the distributor node to download a video file from another node.

    If the node was not in control the specified file and a download is started,
    this method will wait until it is fully finished for no longer than 1 hour.

    Args:
        request:
            Self-explanatory
        _jwt_data:
            Not considered (required for the auth. decorator)
        data:
            Instance of the `DistributorCopyFile` model after parsing the
            request body specifying the video file size and the source node to
            use.

    Raises:
        HTTPOk:
            If and when the download is successfully finished and the node
            controls the video file or if the node already had control of the
            file and therefore no download was necessary.
        HTTPInternalServerError:
            If the file was being copied, but waiting for it to finish took
            longer than 1 hour.
    """
    hash_, ext = request.match_info["hash"], request.match_info["file_ext"]
    file_controller = DistributorFileController.get_instance()
    file_controller.copy_file(hash_, ext, data.from_base_url, data.file_size)
    successful = await file_controller.file_exists(hash_, timeout=3600)
    raise HTTPOk() if successful else HTTPInternalServerError()


@routes.post(r'/api/distributor/delete')
@ensure_auth(Role.node)
@ensure_json_body
async def delete_files(
    _request: Request,
    _jwt_data: RequestJWTData,
    data: DistributorDeleteFiles,
) -> Response:
    """
    Deletes all video files specified in the request body from the node.

    Args:
        _request:
            Not considered (required for the route decorator)
        _jwt_data:
            Not considered (required for the auth. decorator)
        data:
            Instance of the `DistributorDeleteFiles` model after parsing the
            request body specifying the video files to delete.
            Note: Only the specified file hashes are considered; any provided
            file extensions are ignored.
            If `safe` is set to `True`, the node will consider the
            `distribution.last_request_safety_minutes` setting and not delete
            files that have been requested too recently.

    Returns:
        A `Response` built from the `DistributorDeleteFilesResponse` model.
        The `files_skipped` field of the response body will list those files
        that the node did not control at the time of the request as well as
        any files that were skipped honoring the `safe` request parameter.
        The `free_space` field of the response body will contain the total free
        space on the node in MB after successful deletion.
    """
    file_controller = DistributorFileController.get_instance()
    files_skipped: HashedFilesList = []
    for file in data.files:
        try:
            await file_controller.delete_file(file.hash, safe=data.safe)
        except (NoSuchFile, NotSafeToDelete) as e:
            log.info(f"Skipping deletion: {e}")
            files_skipped.append(file)
    free_space = await file_controller.get_free_space()
    return DistributorDeleteFilesResponse(
        files_skipped=files_skipped,
        free_space=free_space,
    ).json_response()


@routes.get(r'/file')
@ensure_auth(Role.client)
async def request_file(
    request: Request,
    jwt_data: RequestFileJWTData,
) -> Union[Response, FileResponse]:
    """
    Serves a video file with the hash specified in the JWT payload.

    Args:
        request:
            Self-explanatory
        jwt_data:
            Instance of the `RequestFileJWTData` model with the `hash` of the
            desired video file. If the `type` is anything but `video` a 404 is
            raised.

    Returns:
        If an `x_accel_location` was configured, a `Response` that can be
        processed by an NGINX reverse proxy to serve the desired file is
        returned. Otherwise, a `FileResponse` to serve the file directly is
        returned.

    Raises:
        HTTPNotFound:
            If the `type` of the JWT payload is anything other than `video`
            or no file with the `hash` specified in the JWT is found.
        HTTPServiceUnavailable:
            If the node is currently downloading the requested file from another
            node, but too many other clients are also waiting for the download
            to be finished, _or_ after waiting 60 seconds, the download is still
            not finished.
    """
    file_controller = DistributorFileController.get_instance()
    if jwt_data.type != FileType.VIDEO:
        log.info(f"Invalid request type: {jwt_data.type}")
        # TODO: This should probably be a 406 instead of a 404.
        raise HTTPNotFound()
    try:
        file = await file_controller.get_file(jwt_data.hash)
    except NoSuchFile as e:
        log.info(f"File request denied: {e}")
        raise HTTPNotFound() from None
    except asyncio.TimeoutError:
        log.warning(f"File request timed out: Still copying {jwt_data.hash}")
        raise HTTPServiceUnavailable() from None
    except TooManyWaitingClients as e:
        log.warning(f"File request failed: {e}")
        raise HTTPServiceUnavailable() from None
    file.set_requested_time()
    download_filename = request.query.get('downloadas')
    if not settings.webserver.x_accel_location:
        return FileResponse(
            file_controller.get_path(file),
            headers=file_serve_headers(download_filename),
        )
    uri = Path(settings.webserver.x_accel_location, rel_path(str(file)))
    limit_rate = settings.webserver.get_x_accel_limit_rate(
        internal=jwt_data.internal
    )
    return serve_file_via_x_accel(uri, limit_rate, download_filename)
