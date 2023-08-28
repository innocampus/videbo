from collections.abc import Callable, Iterable
from functools import wraps
from typing import Optional

from aiohttp.typedefs import Handler
from aiohttp.web_exceptions import HTTPException
from aiohttp.web_request import Request
from aiohttp.web_response import Response, StreamResponse
from aiohttp.web_routedef import RouteTableDef as AIOHTTPRouteTableDef


__all__ = [
    "RouteTableDef",
]


ACCESS_CONTROL_MAX_AGE: int = 60 * 60  # 1 hour


class RouteTableDef(AIOHTTPRouteTableDef):
    def route_with_cors(
        self,
        path: str,
        *allow_methods: str,
        allow_headers: Optional[Iterable[str]] = None,
    ) -> Callable[[Handler], Handler]:
        """
        Decorator for routes with Cross-Origin Resource Sharing (CORS).

        Registers routes for an arbitrary number of HTTP methods.
        Adds all necessary "Access-Control"-headers to the response returned
        or the exception raised by the route handler function.
        Finally, an OPTIONS route is always created with those same headers.

        Args:
            path:
                The URL path to register the route(s) for
            *allow_methods (optional):
                Each argument should be the name of an HTTP method to
                register a route with; one route with the `OPTIONS` method
                is always registered at the end.
            allow_headers (optional):
                Can be passed an iterable of strings representing header
                fields to allow for the route; if provided, a corresponding
                "Access-Control-Allow-Headers" header is added.

        Returns:
            The decorator that wraps the actual route handler function.
            The wrapper is type safe with regular aiohttp route handlers.
        """
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": ",".join(allow_methods),
            "Access-Control-Max-Age": str(ACCESS_CONTROL_MAX_AGE),
        }
        if allow_headers:
            headers["Access-Control-Allow-Headers"] = ",".join(allow_headers)

        async def options_cors_handler(_request: Request) -> StreamResponse:
            return Response(headers=headers)

        def decorator(function: Handler) -> Handler:
            """Internal decorator function"""
            @wraps(function)
            async def wrapper(request: Request) -> StreamResponse:
                """Wrapper around the actual function call"""
                try:
                    response = await function(request)
                except HTTPException as error:
                    error.headers.extend(headers)
                    raise error
                else:
                    response.headers.extend(headers)
                    return response
            for method in allow_methods:
                self.route(method, path)(wrapper)
            self.options(path)(options_cors_handler)
            return wrapper
        # This makes the OPTIONS route handler testable:
        decorator.__options_handler__ = options_cors_handler  # type: ignore[attr-defined]
        return decorator

    def get_with_cors(
        self,
        path: str,
        *,
        allow_headers: Optional[Iterable[str]] = None,
    ) -> Callable[[Handler], Handler]:
        """Convenience method for `route_with_cors` with GET"""
        return self.route_with_cors(path, "GET", allow_headers=allow_headers)

    def post_with_cors(
        self,
        path: str,
        *,
        allow_headers: Optional[Iterable[str]] = None,
    ) -> Callable[[Handler], Handler]:
        """Convenience method for `route_with_cors` with POST"""
        return self.route_with_cors(path, "POST", allow_headers=allow_headers)
