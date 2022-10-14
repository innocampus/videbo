import logging
import functools
import urllib.parse
from collections.abc import AsyncIterator, Callable, Iterable
from pathlib import Path
from json import JSONDecodeError
from typing import Any, Optional, Union, cast, overload

from aiohttp.log import access_logger as aiohttp_access_logger
from aiohttp.typedefs import LooseHeaders
from aiohttp.web import run_app
from aiohttp.web_app import Application
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest
from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from aiohttp.web_routedef import RouteTableDef
from pydantic import ValidationError

from videbo.client import Client
from videbo.misc import MEGA
from videbo.misc.functions import sanitize_filename, get_route_model_param
from videbo.misc.task_manager import TaskManager
from videbo.models import BaseRequestModel
from videbo.types import CleanupContext, RouteHandler
from videbo.video import get_content_type_for_extension


log = logging.getLogger(__name__)


async def session_context(_app: Application) -> AsyncIterator[None]:
    """
    Ensures that all HTTP client sessions are closed on the second iteration.
    Can be used in the `.cleanup_ctx` list of the aiohttp `Application`.
    """
    yield  # no start-up needed
    await Client.close_all()


async def cancel_tasks(_app: Application) -> None:
    TaskManager.cancel_all()


def start_web_server(routes: RouteTableDef, *cleanup_contexts: CleanupContext, address: Optional[str] = None,
                     port: Optional[int] = None, access_logger: logging.Logger = aiohttp_access_logger,
                     verbose: bool = False) -> None:
    """
    Starts the aiohttp web server.
    Adds a context, such that on startup the `HTTPClient.session` is initialized, and on cleanup it is closed.
    Also ensures that all tasks are cancelled on shutdown.

    Args:
        routes:
            The route definition table to serve
        cleanup_contexts (optional):
            Callables that will be added to the `aiohttp.web.Application.cleanup_ctx`;
            should be asynchronous generator functions that take only the app itself as an argument and
            are structured as "startup code; yield; cleanup code".
            (see https://docs.aiohttp.org/en/stable/web_advanced.html#cleanup-context)
        address (optional):
            Passed as the `host` argument to the `run_app` method.
        port (optional):
            Passed as the `port` argument to the `run_app` method.
        access_logger (optional):
            Passed as the `access_log` argument to the `run_app` method; if omitted, the aiohttp.access logger is used.
        verbose (optional):
            If `False` and no `access_logger` was specified, the default logger's level is raised to ERROR.
    """
    app = Application()
    app.add_routes(routes)
    app.cleanup_ctx.append(session_context)
    app.cleanup_ctx.extend(cleanup_contexts)
    app.on_shutdown.append(cancel_tasks)  # executed **before** cleanup
    if not verbose:
        access_logger.setLevel(logging.ERROR)
    run_app(app, host=address, port=port, access_log=access_logger)


# TODO: Annotate with `ParamSpec`
@overload
def ensure_json_body(_func: RouteHandler) -> RouteHandler:
    ...


@overload
def ensure_json_body(*, headers: Optional[LooseHeaders] = None) -> Callable[[RouteHandler], RouteHandler]:
    ...


def ensure_json_body(_func: Optional[RouteHandler] = None, *,
                     headers: Optional[LooseHeaders] = None) -> Union[RouteHandler, Callable[[RouteHandler], RouteHandler]]:
    """
    Decorator function used to ensure that there is a json body in the request and that this json
    corresponds to the model given as a type annotation in func.

    Use `BaseRequestModel` as base class for your models.

    Args:
        _func:
            Control parameter; allows using the decorator with or without arguments.
            If this decorator is used with any arguments, this will always be the decorated function itself.
        headers (optional):
            Headers to include when sending error responses.
    """
    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        param_name, param_class = get_route_model_param(function, BaseRequestModel)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            assert request._client_max_size > 0
            if request.content_type != 'application/json':
                log.info('Wrong content type, json expected, got %s', request.content_type)
                raise HTTPBadRequest(headers=headers)
            try:
                json = await request.json()
                data = param_class.parse_obj(json)
            except ValidationError as error:
                log.info('JSON in request does not match model: %s', str(error))
                raise HTTPBadRequest(headers=headers)
            except JSONDecodeError:
                log.info('Invalid JSON in request')
                raise HTTPBadRequest(headers=headers)
            kwargs[param_name] = data
            return await function(request, *args, **kwargs)
        return cast(RouteHandler, wrapper)

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def file_serve_headers(downloadas: Optional[str] = None) -> dict[str, str]:
    """
    Returns a dictionary of HTTP headers to use in file serving responses.

    The 'Cache-Control' header is always set;
    if `downloadas` is provided, the 'Content-Disposition' header is also set accordingly.
    """
    headers = {
        'Cache-Control': 'private, max-age=50400'
    }
    if downloadas:
        headers['Content-Disposition'] = f'attachment; filename="{urllib.parse.quote(sanitize_filename(downloadas))}"'
    return headers


def get_x_accel_headers(redirect_uri: str, limit_rate_bytes: Optional[int] = None) -> dict[str, str]:
    """
    Returns a dictionary of HTTP headers to use for reverse proxy setups.

    The 'X-Accel-Redirect' header is always set to `redirect_uri`;
    if `limit_rate_bytes` is provided, the 'X-Accel-Limit-Rate' header is also set accordingly.
    """
    headers = {'X-Accel-Redirect': redirect_uri}
    if limit_rate_bytes:
        headers['X-Accel-Limit-Rate'] = str(limit_rate_bytes)
    return headers


def file_serve_response(path: Path, x_accel: bool, downloadas: Optional[str] = None,
                        x_accel_limit_rate: float = 0.0) -> Union[Response, FileResponse]:
    """
    Constructs a response object to serve a file either "as is" or via NGINX X-Accel capabilities.

    Args:
        path:
            Either the actual full path to the file or the 'X-Accel-Redirect' URI
        x_accel:
            If `True`, the response will not reference the file directly, but instead contain relevant X-Accel headers;
            otherwise a `FileResponse` is returned.
        downloadas (optional):
            The `downloadas` value of the request's query
        x_accel_limit_rate (optional):
            'X-Accel-Limit-Rate' header value in megabits (2^20 bits) per second; ignored if `x_accel` is False.

    Returns:
        An appropriately constructed `aiohttp.web.Response` object, if X-Accel is to be used,
        and a `aiohttp.web.FileResponse` object for the provided file path otherwise.
    """
    headers = file_serve_headers(downloadas)
    if x_accel:
        limit_rate_bytes = int(x_accel_limit_rate * MEGA / 8)
        headers.update(get_x_accel_headers(str(path), limit_rate_bytes))
        content_type = get_content_type_for_extension(''.join(path.suffixes))
        return Response(headers=headers, content_type=content_type)
    return FileResponse(path, headers=headers)


def route_with_cors(routes: RouteTableDef, path: str, *allow_methods: str,
                    allow_headers: Optional[Iterable[str]] = None) -> Callable[[RouteHandler], RouteHandler]:
    """
    Decorator function used to register a route with Cross-Origin Resource Sharing (CORS) header fields to the response.

    It also registers a route for the path with the OPTIONS method that responds with the same headers.

    Args:
        routes:
            The `aiohttp.web_routedef.RouteTableDef` instance to use for registering the route
        path:
            The URL path to register the route for
        *allow_methods:
            Each argument should be the name of an HTTP method to register the route with;
            a route with the `OPTIONS` method is always registered.
        allow_headers (optional):
            Can be passed an iterable of strings representing header fields to allow for the route;
            if provided, a corresponding 'Access-Control-Allow-Headers' header will be added to the response.
    """
    def decorator(function: RouteHandler) -> RouteHandler:
        """Internal decorator function"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': ','.join(allow_methods),
            'Access-Control-Max-Age': '3600',
        }
        if allow_headers:
            headers['Access-Control-Allow-Headers'] = ','.join(allow_headers)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call"""
            try:
                response = await function(request, *args, **kwargs)
                response.headers.extend(headers)
                return response
            except HTTPException as error:
                error.headers.extend(headers)
                raise error

        async def return_options(_request: Request) -> Response:
            return Response(headers=headers)

        for method in allow_methods:
            routes.route(method, path)(wrapper)
        routes.route('OPTIONS', path)(return_options)

        return cast(RouteHandler, wrapper)
    return decorator
