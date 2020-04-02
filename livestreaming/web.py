import functools
import inspect
import logging
import pydantic
from json import JSONDecodeError
from typing import Optional, Type, List, Union, Any, Tuple, Callable
from aiohttp import web, ClientResponse, ClientSession, ClientError
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest
from livestreaming.auth import BaseJWTData, internal_jwt_encode

web_logger = logging.getLogger('livestreaming-web')


def start_web_server(port: int, routes, on_startup: Optional[Callable] = None, on_cleanup: Optional[Callable] = None):
    HTTPClient.create_client_session()

    app = web.Application()
    app.add_routes(routes)
    if on_startup:
        app.on_startup.append(on_startup)
    if on_cleanup:
        app.on_cleanup.append(on_cleanup)
    app.on_shutdown.append(HTTPClient.close_all)
    web.run_app(app, port=port)


def ensure_json_body(headers: Optional[dict] = None):
    """Decorator function used to ensure that there is a json body in the request and that this json
    corresponds to the model given as a type annotation in func.

    Use JSONBaseModel as base class for your models.

    On an error, headers can be sent along the response.
    """
    def decorator(func):
        """internal decorator function"""

        # Look for the model given in a type annotation.
        signature = inspect.signature(func)
        param: inspect.Parameter
        model_arg_name = None
        model_arg_model: Optional[Type[pydantic.BaseModel]] = None
        for name, param in signature.parameters.items():
            if issubclass(param.annotation, JSONBaseModel):
                if model_arg_name:
                    raise TooManyJSONModelsError()
                model_arg_name = name
                model_arg_model = param.annotation

        if model_arg_name is None:
            raise NoJSONModelFoundError()

        @functools.wraps(func)
        async def wrapper(request: web.Request, *args, **kwargs):
            """Wrapper around the actual function call."""

            assert request._client_max_size > 0
            if request.content_type != 'application/json':
                web_logger.info('Wrong content type, json expected, got %s', request.content_type)
                raise HTTPBadRequest(headers=headers)

            try:
                json = await request.json()
                data = model_arg_model.parse_obj(json)

            except pydantic.ValidationError as error:
                web_logger.info('JSON in request does not match model: %s', str(error))
                raise HTTPBadRequest(headers=headers)
            except JSONDecodeError:
                web_logger.info('Invalid JSON in request')
                raise HTTPBadRequest(headers=headers)

            kwargs[model_arg_name] = data
            return await func(request, *args, **kwargs)

        return wrapper
    return decorator


class JSONBaseModel(pydantic.BaseModel):
    pass


def register_route_with_cors(routes: web.RouteTableDef, allow_methods: Union[str, List[str]], path: str,
                             allow_headers: Optional[List[str]] = None):
    """Decorator function used to add Cross-Origin Resource Sharing (CORS) header fields to the responses.

    It also registers a route for the path with the OPTIONS method.
    """
    if isinstance(allow_methods, str):
        allow_methods = [allow_methods]

    def decorator(func):
        """internal decorator function"""

        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': ','.join(allow_methods),
            'Access-Control-Max-Age': '3600',
        }

        if allow_headers:
            headers['Access-Control-Allow-Headers'] = ','.join(allow_headers)

        @functools.wraps(func)
        async def wrapper(request: web.Request, *args, **kwargs):
            """Wrapper around the actual function call."""

            try:
                response = await func(request, *args, **kwargs)
                response.headers.extend(headers)
                return response
            except HTTPException as error:
                error.headers.extend(headers)
                raise error

        async def return_options(request: web.Request):
            return web.Response(headers=headers)

        for method in allow_methods:
            routes._items.append(web.RouteDef(method, path, wrapper, {}))
        routes._items.append(web.RouteDef('OPTIONS', path, return_options, {}))

        return wrapper
    return decorator


def json_response(data: JSONBaseModel, status=200) -> web.Response:
    return web.Response(text=data.json(), status=status, content_type='application/json')


class HTTPClient:
    session: ClientSession

    @classmethod
    def create_client_session(cls):
        cls.session = ClientSession() # TODO use TCPConnector and use limit_per_host option

    @classmethod
    async def close_all(cls, app: web.Application):
        await cls.session.close()

    @classmethod
    async def internal_request(cls, method: str, url: str, jwt_data: Optional[BaseJWTData] = None,
                               json_data: Optional[JSONBaseModel] = None,
                               expected_return_type: Optional[Type[JSONBaseModel]] = None) -> Tuple[int, Any]:
        """Do an internal HTTP request, i.e. a request to another node with a JWT using the internal secret.

        You may transmit json data and specify the expected return type."""

        headers = {}
        data = None
        if jwt_data:
            jwt = internal_jwt_encode(jwt_data)
            headers['Authorization'] = "Bearer " + jwt
        if json_data:
            headers['Content-Type'] = 'application/json'
            data = json_data.json()

        try:
            async with cls.session.request(method, url, data=data, headers=headers) as response:
                if response.content_type == 'application/json':
                    json = await response.json()
                    if expected_return_type:
                        return response.status, expected_return_type.parse_obj(json)
                    else:
                        return response.status, json
                elif expected_return_type:
                    web_logger.warning("Got unexpected data while internal web request")
                    raise HTTPResponseError()
                else:
                    some_data = await response.read()
                    return response.status, some_data
        except (ClientError, UnicodeDecodeError, pydantic.ValidationError, JSONDecodeError) as error:
            web_logger.warning("Error while internal web request: " + str(error))
            raise HTTPResponseError()


async def read_data_from_response(response: ClientResponse, max_bytes: int) -> bytes:
    """Read up to max_bytes of data in memory. Be carefull with max_bytes."""
    read_bytes = 0
    blocks = []
    while True:
        block = await response.content.readany()
        if not block:
            break
        read_bytes += len(block)
        if read_bytes > max_bytes:
            raise ResponseTooManyDataError()
        blocks.append(block)
    return b''.join(blocks)


def ensure_url_does_not_end_with_slash(url: str) -> str:
    while url:
        if url[-1] == '/':
            url = url[0:-1]
        else:
            return url


# exceptions
class NoJSONModelFoundError(Exception):
    pass


class TooManyJSONModelsError(Exception):
    pass


class ResponseTooManyDataError(Exception):
    pass


class HTTPResponseError(Exception):
    pass
