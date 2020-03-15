import functools
import inspect
import logging
import pydantic
from json import JSONDecodeError
from typing import Optional, Type, List, Union
from aiohttp import web
from aiohttp.web_exceptions import HTTPException, HTTPBadRequest

web_logger = logging.getLogger('livestreaming-web')


def start_web_server(port: int, routes):
    app = web.Application()
    app.add_routes(routes)
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


# exceptions
class NoJSONModelFoundError(Exception):
    pass


class TooManyJSONModelsError(Exception):
    pass
