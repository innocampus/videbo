from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from json.decoder import JSONDecodeError
from time import time
from types import TracebackType
from typing import Any, Optional, Type, TypeVar, Union, overload

from aiohttp.client import ClientResponse, ClientSession, ClientTimeout
from aiohttp.client_exceptions import ClientError
from pydantic import ValidationError

from videbo.exceptions import HTTPResponseError
from videbo.models import BaseRequestModel, BaseResponseModel, RequestJWTData, Role, TokenIssuer


log = logging.getLogger(__name__)

C = TypeVar("C", bound="Client")
E = TypeVar("E", bound=BaseException)
R = TypeVar("R", bound=BaseResponseModel)

_4HOURS: float = 4. * 60 * 60
_15MINUTES: float = 15. * 60

connection_errors = (
    ClientError,
    ConnectionError,
    JSONDecodeError,
    ValidationError,
    UnicodeDecodeError,
)


class Client:
    _instances: list[Client] = []

    def __init__(self, **session_kwargs: Any) -> None:
        self._session_kwargs: dict[str, Any] = session_kwargs
        self._session: ClientSession = ClientSession(**self._session_kwargs)
        # Mapping of (role, int|ext) -> (jwt, expiration date):
        self._jwt_cache: dict[tuple[Role, TokenIssuer], tuple[str, float]] = {}
        self.__class__._instances.append(self)

    async def renew_session(self, **override_session_kwargs: Any) -> None:
        await self._session.close()
        self._session_kwargs |= override_session_kwargs
        self._session = ClientSession(**self._session_kwargs)

    async def close(self) -> None:
        await self._session.close()

    async def __aenter__(self: C) -> C:
        return self

    async def __aexit__(
        self,
        exc_type: type[E],
        exc_val: E,
        exc_tb: TracebackType,
    ) -> None:
        await self.close()

    @classmethod
    async def close_all(cls) -> None:
        for client in cls._instances:
            await client.close()
        log.info("Closed all HTTP client sessions")

    def get_jwt(self, role: Role, external: bool = False) -> str:
        """
        Returns a JWT from `RequestJWTData` with the provided role.

        If a matching, non-expired token is present in the instance's cache,
        that token is returned. Otherwise, a new token with the specified
        parameters is created, cached, and returned.

        Args:
            role:
                Passed to the `role` field of the JWT.
            external (optional):
                If `True`, `iss=external` is set on the token;
                otherwise `iss=external` is set.

        Returns:
            The token as a string
        """
        if external:
            iss = TokenIssuer.external
        else:
            iss = TokenIssuer.internal
        current_time = time()
        jwt, expiration = self._jwt_cache.get((role, iss), ('', 0))
        if jwt and current_time < expiration:
            return jwt
        jwt = RequestJWTData(
            exp=int(current_time + _4HOURS),
            iss=iss,
            role=role,
        ).encode()
        self._jwt_cache[(role, iss)] = (jwt, current_time + _4HOURS)
        return jwt

    def get_jwt_node(self, external: bool = False) -> str:
        return self.get_jwt(Role.node, external=external)

    def get_jwt_admin(self, external: bool = False) -> str:
        return self.get_jwt(Role.admin, external=external)

    @staticmethod
    async def handle_response(
        response: ClientResponse,
        expect_json: bool = False,
    ) -> Union[dict[str, Any], bytes]:
        if response.content_type == "application/json":
            return await response.json()  # type: ignore[no-any-return]
        if expect_json:
            log.warning(f"Unexpected data during web request to {response.url}")
            raise HTTPResponseError()
        return await response.read()

    @overload
    async def request(
        self,
        method: str,
        url: str,
        jwt: Union[RequestJWTData, str, None] = None,
        *,
        data: Optional[BaseRequestModel] = None,
        return_model: Type[R],  # determines the class of the returned data to be `R`
        timeout: Union[ClientTimeout, float] = _15MINUTES,
        external: bool = False,
        log_connection_error: bool = True,
        headers: Optional[dict[str, str]] = None,
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
        timeout: Union[ClientTimeout, float] = _15MINUTES,
        external: bool = False,
        log_connection_error: bool = True,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[int, Any]:
        ...

    async def request(
        self,
        method: str,
        url: str,
        jwt: Union[RequestJWTData, str, None] = None,
        *,
        data: Optional[BaseRequestModel] = None,
        return_model: Optional[Type[BaseResponseModel]] = None,
        timeout: Union[ClientTimeout, float] = _15MINUTES,
        external: bool = False,
        log_connection_error: bool = True,
        headers: Optional[dict[str, str]] = None,
    ) -> tuple[int, Any]:
        if headers is None:
            headers = {}
        if jwt is not None:
            if isinstance(jwt, RequestJWTData):
                jwt = jwt.encode()
            key = "X-Authorization" if external else "Authorization"
            headers[key] = "Bearer " + jwt
        if data is not None:
            headers["Content-Type"] = "application/json"
        if isinstance(timeout, float):
            timeout = ClientTimeout(total=timeout)
        try:
            async with self._session.request(
                method,
                url,
                data=None if data is None else data.json(),
                headers=headers,
                timeout=timeout,
            ) as response:
                response_data = await self.handle_response(
                    response,
                    expect_json=return_model is not None,
                )
        except connection_errors as e:
            if log_connection_error:
                log.exception(f"{e.__class__.__name__} during web request to {url}")
            raise HTTPResponseError() from e
        if isinstance(return_model, type) and issubclass(return_model, BaseResponseModel):
            return response.status, return_model.parse_obj(response_data)
        return response.status, response_data

    async def request_file_read(
        self,
        url: str,
        jwt: RequestJWTData,
        *,
        chunk_size: int = -1,
        timeout: Union[ClientTimeout, float] = _15MINUTES,
        headers: Optional[dict[str, str]] = None,
    ) -> AsyncIterator[bytes]:
        if headers is None:
            headers = {}
        headers["Authorization"] = "Bearer " + jwt.encode()
        async with self._session.request(
            "GET",
            url,
            headers=headers,
            timeout=timeout,
        ) as response:
            if response.status != 200:
                log.error(
                    "HTTP status %s while requesting file from %s",
                    response.status,
                    url,
                )
                raise HTTPResponseError()
            async for data in response.content.iter_chunked(chunk_size):
                yield data
