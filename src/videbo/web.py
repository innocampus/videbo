import logging
import urllib.parse
from collections.abc import Callable, Iterable
from functools import wraps
from pathlib import Path
from json import JSONDecodeError
from typing import Any, Optional, Union, overload

from aiohttp.log import access_logger as aiohttp_access_logger
from aiohttp.typedefs import LooseHeaders
from aiohttp.web import run_app
from aiohttp.web_app import Application
from aiohttp.web_exceptions import HTTPBadRequest
from aiohttp.web_request import Request
from aiohttp.web_response import Response, StreamResponse
from aiohttp.web_routedef import RouteTableDef
from pydantic import ValidationError

from videbo.client import Client
from videbo.misc.functions import (
    get_route_model_param,
    mime_type_from_file_name,
    sanitize_filename,
)
from videbo.misc.task_manager import TaskManager
from videbo.models import BaseRequestModel
from videbo.types import CleanupContext, ExtendedHandler, StrDict


log = logging.getLogger(__name__)

REQUEST_BODY_MAX_SIZE: float = 256. * 1024  # 256 kB
CACHE_CONTROL_MAX_AGE: int = 14 * 60 * 60  # 14 hours


def get_application(
    cleanup_contexts: Iterable[CleanupContext] = (),
    **kwargs: Any,
) -> Application:
    """
    Returns an `aiohttp.web.Application` instance with the given parameters.

    Always adds the `Client.app_context` as the first cleanup context to
    the `Application.cleanup_ctx` list.
    (see https://docs.aiohttp.org/en/stable/web_advanced.html#cleanup-context)

    `TaskManager.shutdown` is set to be executed immediately on shutdown.

    Args:
        cleanup_contexts (optional):
            Callables that will be added to the `Application.cleanup_ctx`;
            should be asynchronous generator functions that take only the
            app itself as an argument and are structured as
            "startup code; yield; cleanup code".
        **kwargs (optional):
            Passed to the `Application` constructor; by default, only the
            `client_max_size` is set to the constant `REQUEST_BODY_MAX_SIZE`.

    Returns:
        Configured instance of `aiohttp.web.Application`
    """
    kwargs.setdefault("client_max_size", REQUEST_BODY_MAX_SIZE)
    app = Application(**kwargs)
    app.cleanup_ctx.append(Client.app_context)
    app.cleanup_ctx.extend(cleanup_contexts)
    app.on_shutdown.append(TaskManager.shutdown)  # executed before cleanup
    return app


def start_web_server(
    routes: RouteTableDef,
    address: Optional[str] = None,
    port: Optional[int] = None,
    *,
    cleanup_contexts: Iterable[CleanupContext] = (),
    access_logger: logging.Logger = aiohttp_access_logger,
    verbose: bool = False,
    **app_kwargs: Any,
) -> None:
    """
    Configures and starts the `aiohttp` web server.

    Calls `get_application` to set up `aiohttp.web.Application` instance.
    Ensures that before exiting the program, all client sessions are closed
    and all tasks are cancelled.

    Args:
        routes:
            The route definition table to serve
        address (optional):
            Passed as the `host` argument to the `run_app` method.
        port (optional):
            Passed as the `port` argument to the `run_app` method.
        cleanup_contexts (optional):
            Passed to `get_application`
        access_logger (optional):
            Passed as the `access_log` argument to the `run_app` method;
            if omitted, the `aiohttp.log.access_logger` is used.
        verbose (optional):
            If `False` (default), the `access_logger` level is set to ERROR.
        **app_kwargs (optional):
            Passed to `get_application`
    """
    app = get_application(cleanup_contexts=cleanup_contexts, **app_kwargs)
    app.add_routes(routes)
    if not verbose:
        access_logger.setLevel(logging.ERROR)
    run_app(app, host=address, port=port, access_log=access_logger)


@overload
def ensure_json_body(_func: ExtendedHandler) -> ExtendedHandler:
    ...


@overload
def ensure_json_body(
    *,
    headers: Optional[LooseHeaders] = None,
) -> Callable[[ExtendedHandler], ExtendedHandler]:
    ...


def ensure_json_body(
    _func: Optional[ExtendedHandler] = None,
    *,
    headers: Optional[LooseHeaders] = None,
) -> Union[ExtendedHandler, Callable[[ExtendedHandler], ExtendedHandler]]:
    """
    Decorator for route handlers validating the JSON body of the request.

    The (extended) route handler function must have an additional parameter
    of annotated with `BaseRequestModel` or a subclass, which should
    represent the expected schema of the JSON payload.
    The wrapper around the handler function will then first attempt to parse
    the payload through that model.
    If the payload is malformed or does not pass validation, the wrapper
    will raise HTTP 400.
    Otherwise the data object will be passed as the appropriate keyword
    argument to the actual handler function.

    Args:
        _func:
            Control parameter that allows using the decorator with arguments
            and also entirely without parentheses. If used without
            parentheses, this will always be the decorated function itself.
        headers (optional):
            Headers to include when sending error responses.

    Returns:
        The internal decorator that wraps the actual route handler function
        or the wrapper right away, if the "no-parentheses" notation is used.
        The wrapper is NOT type safe to pass to aiohttp route definitions.
        (It accepts additional arguments besides the requests.)
    """
    def decorator(function: ExtendedHandler) -> ExtendedHandler:
        """Internal decorator function"""
        param_name, cls = get_route_model_param(function, BaseRequestModel)

        @wraps(function)
        async def wrapper(
            request: Request,
            *args: Any,
            **kwargs: Any,
        ) -> StreamResponse:
            """Wrapper around the actual function call."""
            if request.content_type != "application/json":
                log.info(
                    f"Request has wrong content type; 'application/json' "
                    f"expected, but received '{request.content_type}'"
                )
                raise HTTPBadRequest(headers=headers)
            try:
                data = await request.json()
            except JSONDecodeError:
                log.info("Request body is not valid JSON")
                raise HTTPBadRequest(headers=headers)
            try:
                kwargs[param_name] = cls.parse_obj(data)
            except ValidationError as error:
                log.info(f"JSON in request does not match model: {error}")
                raise HTTPBadRequest(headers=headers)
            return await function(request, *args, **kwargs)
        return wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)


def file_serve_headers(download_filename: Optional[str] = None) -> StrDict:
    """
    Returns a dictionary of HTTP headers to use in file serving responses.

    The "Cache-Control" header is always set; if `downloadas` is provided,
    the "Content-Disposition" header is also set accordingly.
    """
    headers = {
        "Cache-Control": f"private, max-age={CACHE_CONTROL_MAX_AGE}"
    }
    if download_filename:
        name = urllib.parse.quote(sanitize_filename(download_filename))
        headers["Content-Disposition"] = f'attachment; filename="{name}"'
    return headers


def x_accel_headers(redirect_uri: str, limit_rate_bytes: int = 0) -> StrDict:
    """
    Returns a dictionary of HTTP headers to use for reverse proxy setups.

    Args:
        redirect_uri:
            Assigned to the "X-Accel-Redirect" header
        limit_rate_bytes (optional):
            Assigned to the "X-Accel-Limit-Rate" header, if greater than 0
    """
    headers = {"X-Accel-Redirect": redirect_uri}
    if limit_rate_bytes > 0:
        headers["X-Accel-Limit-Rate"] = str(limit_rate_bytes)
    return headers


def serve_file_via_x_accel(
    redirect_uri: Path,
    limit_rate_bytes: int = 0,
    download_filename: Optional[str] = None,
) -> Response:
    """
    Returns a `aiohttp.web.Response` to serve a file via NGINX X-Accel.

    Args:
        redirect_uri:
            The path to the file relative to the NGINX X-Accel location
        limit_rate_bytes (optional):
            "X-Accel-Limit-Rate" header value in bytes per second;
            if zero is passed (default) this header will be omitted.
        download_filename (optional):
            If passed, the "Content-Disposition" header is set to
            attachment; filename="{download_filename}"

    Returns:
        A response that can be processed by an NGINX reverse proxy
        to serve the specified file
    """
    headers = file_serve_headers(download_filename)
    headers.update(x_accel_headers(str(redirect_uri), limit_rate_bytes))
    content_type = mime_type_from_file_name(redirect_uri)
    return Response(headers=headers, content_type=content_type)
