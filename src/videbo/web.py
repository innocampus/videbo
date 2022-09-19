import logging
import functools
import urllib.parse
from collections.abc import AsyncIterator, Callable, Iterable
from pathlib import Path
from json import JSONDecodeError
from time import time
from typing import Any, Optional, Type, Union, cast, overload

from aiohttp.client import ClientSession, ClientError, ClientTimeout
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

from videbo.exceptions import HTTPResponseError
from videbo.misc import MEGA
from videbo.misc.functions import sanitize_filename, get_route_model_param
from videbo.misc.task_manager import TaskManager
from videbo.models import JSONBaseModel, TokenIssuer, Role, RequestJWTData
from videbo.types import CleanupContext, RouteHandler
from videbo.video import get_content_type_for_extension


log = logging.getLogger(__name__)


async def session_context(_app: Application) -> AsyncIterator[None]:
    """
    Creates the singleton session instance on the first iteration, and closes it on the second.
    This coroutine can be used in the `.cleanup_ctx` list of the aiohttp `Application`.
    """
    HTTPClient.create_client_session()
    yield
    await HTTPClient.close_all()


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

    Use `JSONBaseModel` as base class for your models.

    Args:
        _func:
            Control parameter; allows using the decorator with or without arguments.
            If this decorator is used with any arguments, this will always be the decorated function itself.
        headers (optional):
            Headers to include when sending error responses.
    """
    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        param_name, param_class = get_route_model_param(function, JSONBaseModel)

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


class HTTPClient:
    session: ClientSession
    _cached_jwt: dict[tuple[Role, TokenIssuer], tuple[str, float]] = {}  # (role, int|ext) -> (jwt, expiration date)

    @classmethod
    def create_client_session(cls) -> None:
        cls.session = ClientSession()  # TODO use TCPConnector and use limit_per_host option

    @classmethod
    async def close_all(cls) -> None:
        await cls.session.close()

    # TODO: Rework type annotations; overload to indicate return type dependence
    @classmethod
    async def videbo_request(cls, method: str, url: str, jwt_data: Union[RequestJWTData, str, None] = None,
                             json_data: Optional[JSONBaseModel] = None,
                             expected_return_type: Optional[Type[JSONBaseModel]] = None,
                             timeout: Union[ClientTimeout, int, None] = None,
                             external: bool = False,
                             print_connection_exception: bool = True) -> tuple[int, Any]:
        """Do a HTTP request, i.e. a request to another node with a JWT using the internal or external secret.

        You may transmit json data and specify the expected return type."""

        headers = {}
        data = None
        if jwt_data:
            if isinstance(jwt_data, RequestJWTData):
                jwt = jwt_data.encode()
            else:
                # Then it is a string. Assume it is a valid jwt.
                jwt = jwt_data

            if external:
                headers["X-Authorization"] = "Bearer " + jwt
            else:
                headers["Authorization"] = "Bearer " + jwt

        if json_data:
            headers['Content-Type'] = 'application/json'
            data = json_data.json()

        if isinstance(timeout, int):
            timeout_obj = ClientTimeout(total=timeout)
        elif isinstance(timeout, ClientTimeout):
            timeout_obj = timeout
        else:
            timeout_obj = ClientTimeout(total=15 * 60)

        try:
            async with cls.session.request(method, url, data=data, headers=headers, timeout=timeout_obj) as response:
                if response.content_type == 'application/json':
                    json = await response.json()
                    if expected_return_type:
                        return response.status, expected_return_type.parse_obj(json)
                    else:
                        return response.status, json
                elif expected_return_type:
                    log.warning(f"Got unexpected data while internal web request ({url}).")
                    raise HTTPResponseError()
                else:
                    some_data = await response.read()
                    return response.status, some_data
        except (ClientError, UnicodeDecodeError, ValidationError, JSONDecodeError, ConnectionError):
            if print_connection_exception:
                log.exception(f"Error while internal web request ({url}).")
            raise HTTPResponseError()

    @classmethod
    async def internal_request_node(cls, method: str, url: str,
                                    json_data: Optional[JSONBaseModel] = None,
                                    expected_return_type: Optional[Type[JSONBaseModel]] = None,
                                    timeout: Union[ClientTimeout, int, None] = None,
                                    print_connection_exception: bool = True) -> tuple[int, Any]:
        """Do an internal request with the node role (without having to specify jwt_data)."""
        jwt = cls.get_standard_jwt_with_role(Role.node)
        return await cls.videbo_request(method, url, jwt, json_data, expected_return_type, timeout,
                                        print_connection_exception=print_connection_exception)

    @classmethod
    async def internal_request_admin(cls, method: str, url: str,
                                     json_data: Optional[JSONBaseModel] = None,
                                     expected_return_type: Optional[Type[JSONBaseModel]] = None,
                                     timeout: Union[ClientTimeout, int, None] = None,
                                     print_connection_exception: bool = True) -> tuple[int, Any]:
        """Do an internal request with the node role (without having to specify jwt_data)."""
        jwt = cls.get_standard_jwt_with_role(Role.admin)
        return await cls.videbo_request(method, url, jwt, json_data, expected_return_type, timeout,
                                        print_connection_exception=print_connection_exception)

    @classmethod
    def get_standard_jwt_with_role(cls, role: Role, external: bool = False) -> str:
        """Return a JWT with the BaseJWTData and just the role.

        Implements a caching mechanism."""

        if external:
            iss = TokenIssuer.external
        else:
            iss = TokenIssuer.internal
        current_time = time()
        jwt, expiration = cls._cached_jwt.get((role, iss), ('', 0))
        if jwt and current_time < expiration:
            return jwt

        jwt = RequestJWTData(
            exp=int(time()) + 4 * 3600,  # expires in 4 hours
            iss=iss,
            role=role
        ).encode()
        # Don't cache until the expiration time is reached:
        cls._cached_jwt[(role, iss)] = (jwt, current_time + 3 * 3600)
        return jwt
