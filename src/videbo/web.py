import logging
import functools
import urllib.parse
from pathlib import Path
from json import JSONDecodeError
from time import time
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Tuple, Type, Union, cast, overload

from aiohttp.client import ClientSession, ClientError, ClientTimeout
from aiohttp.typedefs import LooseHeaders
from aiohttp.web import run_app
from aiohttp.web_app import Application
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest, HTTPUnauthorized
from aiohttp.web_fileresponse import FileResponse
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from aiohttp.web_routedef import RouteDef, RouteTableDef
from pydantic import ValidationError

from videbo.auth import internal_jwt_encode, external_jwt_encode, JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL
from videbo.exceptions import HTTPResponseError
from videbo.misc import TaskManager, sanitize_filename, get_route_model_param
from videbo.models import JSONBaseModel, BaseJWTData
from videbo.types import CleanupContext, RouteHandler
from videbo.video import get_content_type_for_extension


web_logger = logging.getLogger('videbo-web')


async def session_context(_app: Application) -> AsyncIterator[None]:
    """
    Creates the singleton session instance on the first iteration, and closes it on the second.
    This coroutine can be used in the `.cleanup_ctx` list of the aiohttp `Application`.
    """
    HTTPClient.create_client_session()
    yield
    await HTTPClient.close_all()


async def cancel_tasks(_app: Application) -> None: TaskManager.cancel_all()


def start_web_server(routes: RouteTableDef, *cleanup_contexts: CleanupContext, address: Optional[str] = None,
                     port: Optional[int] = None, access_logger: Optional[logging.Logger] = None,
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
    if access_logger is None:
        access_logger = logging.getLogger('aiohttp.access')
        if not verbose:
            access_logger.setLevel(logging.ERROR)
    run_app(app, host=address, port=port, access_log=access_logger)


@overload
def ensure_json_body(_func: RouteHandler) -> RouteHandler: ...


@overload
def ensure_json_body(*, headers: Optional[LooseHeaders] = None) -> Callable[[RouteHandler], RouteHandler]: ...


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
                web_logger.info('Wrong content type, json expected, got %s', request.content_type)
                raise HTTPBadRequest(headers=headers)
            try:
                json = await request.json()
                data = param_class.parse_obj(json)
            except ValidationError as error:
                web_logger.info('JSON in request does not match model: %s', str(error))
                raise HTTPBadRequest(headers=headers)
            except JSONDecodeError:
                web_logger.info('Invalid JSON in request')
                raise HTTPBadRequest(headers=headers)
            kwargs[param_name] = data
            return await function(request, *args, **kwargs)
        return cast(RouteHandler, wrapper)

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def ensure_no_reverse_proxy(func: RouteHandler) -> RouteHandler:
    """Check that the access does not come from the reverse proxy."""

    @functools.wraps(func)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        """Wrapper around the actual function call."""

        if "X-Forwarded-For" in request.headers:
            raise HTTPUnauthorized()
        return await func(request, *args, **kwargs)

    return cast(RouteHandler, wrapper)


def get_x_accel_headers(redirect_uri: str, limit_rate_bytes: Optional[int] = None) -> Dict[str, str]:
    headers = {'X-Accel-Redirect': redirect_uri}
    if limit_rate_bytes:
        headers['X-Accel-Limit-Rate'] = str(limit_rate_bytes)
    return headers


def get_x_accel_limit_rate(in_mbit: Optional[float]) -> int:
    if in_mbit is None:
        return 0
    return int(in_mbit * 2**20 / 8)


def file_serve_response(path: Path, x_accel: bool, downloadas: Optional[str] = None,
                        x_accel_limit_rate: Optional[float] = None) -> Union[Response, FileResponse]:
    """
    Constructs a response object to serve a file either "as is" or via NGINX X-Accel capabilities.

    Args:
        path:
            Either the actual full path to the file or the X-Accel-Redirect URI
        x_accel:
            If `True`, the response will not reference the file directly, but instead contain relevant X-Accel headers;
            otherwise a FileResponse is returned.
        downloadas (optional):
            The `downloadas` value of the request's query
        x_accel_limit_rate (optional):
            Passed to the `get_x_accel_limit_rate` function to get the X-Accel-Limit-Rate value in bytes

    Returns:
        An appropriately constructed `aiohttp.web.Response` object, if X-Accel is to be used,
        and a `aiohttp.web.FileResponse` object for the provided file path otherwise.
    """
    headers = file_serve_headers(downloadas)
    if x_accel:
        headers.update(get_x_accel_headers(str(path), get_x_accel_limit_rate(x_accel_limit_rate)))
        content_type = get_content_type_for_extension(''.join(path.suffixes))
        return Response(headers=headers, content_type=content_type)
    return FileResponse(path, headers=headers)


def file_serve_headers(downloadas: Optional[str] = None) -> Dict[str, str]:
    headers = {
        'Cache-Control': 'private, max-age=50400'
    }
    if downloadas:
        headers['Content-Disposition'] = f'attachment; filename="{urllib.parse.quote(sanitize_filename(downloadas))}"'
    return headers


def register_route_with_cors(routes: RouteTableDef, allow_methods: Union[str, List[str]], path: str,
                             allow_headers: Optional[List[str]] = None) -> Callable[[RouteHandler], RouteHandler]:
    """Decorator function used to add Cross-Origin Resource Sharing (CORS) header fields to the responses.

    It also registers a route for the path with the OPTIONS method.
    """
    if isinstance(allow_methods, str):
        allow_methods = [allow_methods]

    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': ','.join(allow_methods),
            'Access-Control-Max-Age': '3600',
        }
        if allow_headers:
            headers['Access-Control-Allow-Headers'] = ','.join(allow_headers)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            try:
                response = await function(request, *args, **kwargs)
                response.headers.extend(headers)
                return response
            except HTTPException as error:
                error.headers.extend(headers)
                raise error

        async def return_options(request: Request) -> Response:
            return Response(headers=headers)

        for method in allow_methods:
            routes._items.append(RouteDef(method, path, wrapper, {}))
        routes._items.append(RouteDef('OPTIONS', path, return_options, {}))

        return cast(RouteHandler, wrapper)
    return decorator


def json_response(data: JSONBaseModel, status: int = 200) -> Response:
    return Response(text=data.json(), status=status, content_type='application/json')


class HTTPClient:
    session: ClientSession
    _cached_jwt: Dict[Tuple[str, str], Tuple[str, float]] = {}  # (role, int|ext) -> (jwt, expiration date)

    @classmethod
    def create_client_session(cls) -> None:
        cls.session = ClientSession()  # TODO use TCPConnector and use limit_per_host option

    @classmethod
    async def close_all(cls) -> None:
        await cls.session.close()

    @classmethod
    async def videbo_request(cls, method: str, url: str, jwt_data: Union[BaseJWTData, str, None] = None,
                             json_data: Optional[JSONBaseModel] = None,
                             expected_return_type: Optional[Type[JSONBaseModel]] = None,
                             timeout: Union[ClientTimeout, int, None] = None,
                             external: bool = False,
                             print_connection_exception: bool = True) -> Tuple[int, Any]:
        """Do a HTTP request, i.e. a request to another node with a JWT using the internal or external secret.

        You may transmit json data and specify the expected return type."""

        headers = {}
        data = None
        if jwt_data:
            if isinstance(jwt_data, BaseJWTData):
                if external:
                    jwt = external_jwt_encode(jwt_data)
                else:
                    jwt = internal_jwt_encode(jwt_data)
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
            timeout_obj = ClientTimeout(total=15*60)

        try:
            async with cls.session.request(method, url, data=data, headers=headers, timeout=timeout_obj) as response:
                if response.content_type == 'application/json':
                    json = await response.json()
                    if expected_return_type:
                        return response.status, expected_return_type.parse_obj(json)
                    else:
                        return response.status, json
                elif expected_return_type:
                    web_logger.warning(f"Got unexpected data while internal web request ({url}).")
                    raise HTTPResponseError()
                else:
                    some_data = await response.read()
                    return response.status, some_data
        except (ClientError, UnicodeDecodeError, ValidationError, JSONDecodeError, ConnectionError) \
                as error:
            if print_connection_exception:
                web_logger.exception(f"Error while internal web request ({url}).")
            raise HTTPResponseError()

    @classmethod
    async def internal_request_node(cls, method: str, url: str,
                                    json_data: Optional[JSONBaseModel] = None,
                                    expected_return_type: Optional[Type[JSONBaseModel]] = None,
                                    timeout: Union[ClientTimeout, int, None] = None,
                                    print_connection_exception: bool = True) -> Tuple[int, Any]:
        """Do an internal request with the node role (without having to specify jwt_data)."""
        jwt = cls.get_standard_jwt_with_role('node')
        return await cls.videbo_request(method, url, jwt, json_data, expected_return_type, timeout,
                                        print_connection_exception=print_connection_exception)

    @classmethod
    async def internal_request_admin(cls, method: str, url: str,
                                     json_data: Optional[JSONBaseModel] = None,
                                     expected_return_type: Optional[Type[JSONBaseModel]] = None,
                                     timeout: Union[ClientTimeout, int, None] = None,
                                     print_connection_exception: bool = True) -> Tuple[int, Any]:
        """Do an internal request with the node role (without having to specify jwt_data)."""
        jwt = cls.get_standard_jwt_with_role('admin')
        return await cls.videbo_request(method, url, jwt, json_data, expected_return_type, timeout,
                                        print_connection_exception=print_connection_exception)

    @classmethod
    def get_standard_jwt_with_role(cls, role: str, external: bool = False) -> str:
        """Return a JWT with the BaseJWTData and just the role.

        Implements a caching mechanism."""

        if external:
            iss = JWT_ISS_EXTERNAL
        else:
            iss = JWT_ISS_INTERNAL
        current_time = time()
        jwt, expiration = cls._cached_jwt.get((role, iss), ('', 0))
        if jwt and current_time < expiration:
            return jwt

        jwt_data = BaseJWTData.construct(role=role)
        if external:
            jwt = external_jwt_encode(jwt_data, 4 * 3600)
        else:
            jwt = internal_jwt_encode(jwt_data, 4 * 3600)
        cls._cached_jwt[(role, iss)] = (jwt, current_time + 3 * 3600)  # don't cache until the expiration time is reached
        return jwt
