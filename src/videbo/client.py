from __future__ import annotations

import logging
from asyncio.tasks import gather
from collections.abc import AsyncIterator
from json.decoder import JSONDecodeError
from time import time
from types import TracebackType
from typing import Any, ClassVar, Optional, TypeVar, Union, overload

from aiohttp import client as aiohttp_client
from aiohttp.web_app import Application
from pydantic import ValidationError

from videbo.exceptions import HTTPClientError
from videbo.misc.constants import HTTP_CODE_OK
from videbo.misc.functions import is_subclass
from videbo.models import BaseRequestModel, BaseResponseModel, RequestJWTData, Role, TokenIssuer


__all__ = [
    "Client",
    "CONNECTION_ERRORS",
]

log = logging.getLogger(__name__)

C = TypeVar("C", bound="Client")
E = TypeVar("E", bound=BaseException)
R = TypeVar("R", bound=BaseResponseModel)

_4HOURS: float = 4. * 60 * 60
_15MINUTES: float = 15. * 60

CONNECTION_ERRORS = (
    aiohttp_client.ClientError,
    ConnectionError,
    JSONDecodeError,
    ValidationError,
    UnicodeDecodeError,
)


class Client:
    """
    HTTP Client for JWT protected internal and external requests.

    Caches used JSON Web Tokens based on their `Role` and `TokenIssuer`;
    cache stores tokens along with their expiration date-time.
    Serves as a convenience wrapper around the `aiohttp.ClientSession`.
    """
    _instances: ClassVar[list[Client]] = []

    _session_kwargs: dict[str, Any]
    _session: aiohttp_client.ClientSession
    # Mapping of (role, iss) -> (jwt, expiration date):
    _jwt_cache: dict[tuple[Role, TokenIssuer], tuple[str, float]]

    def __init__(self, **session_kwargs: Any) -> None:
        """
        Instantiation immediately starts a new `aiohttp.ClientSession`.

        Keyword arguments for the session instance will be saved,
        in case the session is to be reset at some later point in time.
        """
        self._session_kwargs = session_kwargs
        self._session = aiohttp_client.ClientSession(**self._session_kwargs)
        self._jwt_cache = {}
        self.__class__._instances.append(self)

    async def renew_session(self, **override_session_kwargs: Any) -> None:
        """
        Closes the internal session and starts a new one.

        Keyword arguments for `aiohttp.ClientSession` used during
        initialization will be passed to the constructor again,
        unless overridden via `**override_session_kwargs`.
        """
        await self._session.close()
        self._session_kwargs |= override_session_kwargs
        self._session = aiohttp_client.ClientSession(**self._session_kwargs)

    async def close(self) -> None:
        """Closes the internal session instance."""
        await self._session.close()

    async def __aenter__(self: C) -> C:
        """Allows usage of a client instance as a context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[E]],
        exc_val: Optional[E],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exiting the `async with`-block closes the client session."""
        await self.close()

    @classmethod
    async def close_all(cls) -> None:
        """Closes the session in every client instance."""
        log.debug("Closing all open HTTP client sessions...")
        await gather(*(client.close() for client in cls._instances))
        log.info("HTTP clients closed")

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        """
        Ensures that all HTTP client sessions are closed on the second iteration.
        Can be used in the `.cleanup_ctx` list of the `aiohttp.Application`.
        """
        yield  # no start-up needed
        await cls.close_all()

    def get_jwt(self, role: Role, issuer: TokenIssuer = TokenIssuer.internal) -> str:
        """
        Returns a JWT from `RequestJWTData` with the provided role.

        If a matching, non-expired token is present in the instance's cache,
        that token is returned. Otherwise, a new token with the specified
        parameters is created, cached, and returned.

        Args:
            role:
                Passed to the `role` field of the JWT.
            issuer (optional):
                Passed to the `iss` field of the JWT; defaults to `TokenIssuer.internal`.

        Returns:
            The token as a string
        """
        current_time = time()
        jwt, expiration = self._jwt_cache.get((role, issuer), ('', 0))
        if jwt and current_time < expiration:
            return jwt
        jwt = RequestJWTData(
            exp=int(current_time + _4HOURS),
            iss=issuer,
            role=role,
        ).encode()
        self._jwt_cache[(role, issuer)] = (jwt, current_time + _4HOURS)
        return jwt

    def get_jwt_node(self) -> str:
        """Convenience method to get an internal JWT with the `node` role."""
        return self.get_jwt(Role.node)

    def get_jwt_admin(self) -> str:
        """Convenience method to get an internal JWT with the `admin` role."""
        return self.get_jwt(Role.admin)

    @staticmethod
    def update_auth_header(
        headers: dict[str, Any],
        jwt: Union[RequestJWTData, str, None],
        external: bool = False,
    ) -> None:
        """
        Updates the specified `headers` dict to include the provided `jwt`.

        Args:
            headers:
                The dictionary of HTTP headers to update
            jwt:
                If provided a string, it is assumed to be a valid JWT
                and an 'Authorization' header is added (or overridden)
                with that token prefixed with 'Bearer ';
                if provided an instance of `RequestJWTData`, it is first
                encoded to a string and then added to the headers;
                if `None` nothing is done.
            external (optional):
                If `True` and a `jwt` is provided, it will be added to a
                'X-Authorization' header; if `False` (default) the regular
                'Authorization' header will be used.
        """
        if jwt is None:
            return
        if isinstance(jwt, RequestJWTData):
            jwt = jwt.encode()
        key = "X-Authorization" if external else "Authorization"
        headers[key] = "Bearer " + jwt

    @staticmethod
    async def handle_response(
        response: aiohttp_client.ClientResponse,
        expect_json: bool = False,
    ) -> Union[dict[str, Any], bytes]:
        """
        Returns the body/content of a specified response object.

        Args:
            response:
                Instance of `aiohttp.ClientResponse`; if it has a JSON
                content type, its `.json()` method is called, otherwise
                its `.read()` method is called.
            expect_json (optional):
                If `True` and the content type of the `response` is
                _not_ JSON, an error is raised.

        Returns:
            A dictionary of the response content, if the response content
            type was JSON; otherwise response body as `bytes`.

        Raises:
            `HTTPClientError` if the response does not have the JSON
            content type, but `expect_json` is set to `True`.
        """
        if response.content_type == "application/json":
            return await response.json()  # type: ignore[no-any-return]
        if expect_json:
            log.warning(f"Unexpected data during web request to {response.url}")
            raise HTTPClientError()
        return await response.read()

    @overload
    async def request(
        self,
        method: str,
        url: str,
        jwt: Union[RequestJWTData, str, None] = None,
        *,
        data: Optional[BaseRequestModel] = None,
        return_model: type[R],  # determines the class of the returned data to be `R`
        external: bool = False,
        log_connection_error: bool = True,
        **kwargs: Any,
    ) -> tuple[int, R]:
        ...

    @overload
    async def request(
        self,
        method: str,
        url: str,
        jwt: Union[RequestJWTData, str, None] = None,
        *,
        data: Optional[BaseRequestModel] = None,
        return_model: None = None,  # returned data could be anything
        external: bool = False,
        log_connection_error: bool = True,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        ...

    async def request(
        self,
        method: str,
        url: str,
        jwt: Union[RequestJWTData, str, None] = None,
        *,
        data: Optional[BaseRequestModel] = None,
        return_model: Optional[type[BaseResponseModel]] = None,
        external: bool = False,
        log_connection_error: bool = True,
        **kwargs: Any,
    ) -> tuple[int, Any]:
        """
        Performs an HTTP request with the specified parameters.

        Args:
            method:
                The HTTP method to use
            url:
                The target URL to address
            jwt (optional):
                If provided a string, it is assumed to be a valid JWT
                and an 'Authorization' header is added (or overridden)
                with that token prefixed with 'Bearer ';
                if provided an instance of `RequestJWTData`, it is first
                encoded to a string and then added to the headers;
                if `None` (default) no such header is added.
            data (optional):
                If provided an instance of `BaseRequestModel`, its JSON
                representation is passed as the request payload.
            return_model (optional):
                If provided a subclass of `BaseResponseModel`, the
                response's content type is assumed to be JSON and its body
                will be parsed to return an instance of that class;
                if `None` (default) the response data is returned "as is".
            external (optional):
                If `True` and a `jwt` is provided, it will be added to a
                'X-Authorization' header; if `False` (default) the regular
                'Authorization' header will be used.
            log_connection_error (optional):
                If `True` (default), any connection error will be explicitly
                logged, before an `HTTPClientError` is raised.
            **kwargs (optional):
                Passed to the `aiohttp.ClientSession.request` method;
                must not contain the `data` keyword; if not otherwise
                specified the `timeout` will be set to 15 minutes;
                if provided `headers`, they may be partially overridden.

        Returns:
            A 2-tuple of the HTTP response status code and content data;
            the data will be an instance of `return_model` if provided.

        Raises:
            `HTTPClientError` if one of the `CONNECTION_ERRORS` is caught.
        """
        kwargs.setdefault("timeout", aiohttp_client.ClientTimeout(_15MINUTES))
        kwargs.setdefault("headers", {})
        self.update_auth_header(kwargs["headers"], jwt, external=external)
        if data is not None:
            kwargs["headers"]["Content-Type"] = "application/json"
        try:
            async with self._session.request(
                method,
                url,
                data=None if data is None else data.json(),
                **kwargs,
            ) as response:
                response_data = await self.handle_response(
                    response,
                    expect_json=return_model is not None,
                )
        except CONNECTION_ERRORS as e:
            if log_connection_error:
                log.exception(f"{e.__class__.__name__} during web request to {url}")
            raise HTTPClientError() from e
        if is_subclass(return_model, BaseResponseModel):
            return response.status, return_model.parse_obj(response_data)
        return response.status, response_data

    async def request_file_read(
        self,
        url: str,
        jwt: Union[RequestJWTData, str],
        *,
        chunk_size: int = -1,
        **kwargs: Any,
    ) -> AsyncIterator[bytes]:
        """
        Returns an async iterator for downloading a file from a node.

        Args:
            url:
                The target URL to address
            jwt:
                If provided a string, it is assumed to be a valid JWT
                and an 'Authorization' header is added (or overridden)
                with that token prefixed with 'Bearer ';
                if provided an instance of `RequestJWTData`, it is first
                encoded to a string and then added to the headers.
            chunk_size (optional):
                Determines the size of the chunks of `bytes` data yielded
                by the iterator; `-1` disables chunking and immediately
                yields the entire file.
            **kwargs (optional):
                Passed to the `aiohttp.ClientSession.request` method; if
                not otherwise specified `timeout` will be set to 15 minutes;
                if provided `headers`, they may be partially overridden.
        """
        kwargs.setdefault("timeout", aiohttp_client.ClientTimeout(_15MINUTES))
        kwargs.setdefault("headers", {})
        self.update_auth_header(kwargs["headers"], jwt)
        async with self._session.request("GET", url, **kwargs) as response:
            if response.status != HTTP_CODE_OK:
                log.error(
                    "HTTP status %s while requesting file from %s",
                    response.status,
                    url,
                )
                raise HTTPClientError()
            # The branch coverage exclusion is needed due to a CPython bug:
            # https://github.com/nedbat/coveragepy/issues/1324
            async for data in response.content.iter_chunked(chunk_size):  # pragma: no branch
                yield data
