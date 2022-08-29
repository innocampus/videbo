import logging
import functools
from collections.abc import Callable, Mapping
from typing import Any, Optional, Type, cast

import jwt
from aiohttp.typedefs import LooseHeaders
from aiohttp.web_request import Request
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPForbidden
from pydantic import ValidationError

from videbo import storage_settings as settings
from videbo.exceptions import InvalidAuthData, NotAuthorized
from videbo.misc import get_route_model_param
from videbo.models import TokenIssuer, Role, RequestJWTData
from videbo.types import RouteHandler


__all__ = [
    'ensure_auth'
]


logger = logging.getLogger('videbo-auth')

JWT_ALG = 'HS256'


def extract_jwt_from_request(request: Request) -> str:
    """
    Finds the JSON Web Token in a request and returns it.

    If the "Authorization" header is found, assuming the value starts with "Bearer ", the following string is returned;
    otherwise the request query is assumed to contain a "jwt" field, the value of which is returned.

    Args:
        request: The `aiohttp` request object supposedly containing the JWT.

    Returns:
        The JWT string

    Raises:
        `InvalidAuthData` if the assumptions mentioned above are not satisfied.
    """
    try:
        # First try to find the token in the header.
        header_auth: str = request.headers.getone('Authorization')
    except KeyError:
        # If it is not in the header, try to find the token in the GET field jwt.
        query: Mapping[str, str] = request.query
        if 'jwt' in query:
            return query['jwt']
    else:
        prefix = 'Bearer '
        if header_auth.startswith(prefix):
            return header_auth[len(prefix):]
    raise InvalidAuthData("No JWT found in request.")


def jwt_kid_internal(token: str) -> bool:
    """
    Checks the key ID header of a JSON Web Token and determines if the token should be encoded as internal.

    Args:
        token: The JWT string to check

    Returns:
        `True` if the value of the "kid" header corresponds to `TokenIssuer.internal`.
        `False` if the value of the "kid" header corresponds to `TokenIssuer.external`.

    Raises:
        `InvalidAuthData` if the "kid" header is missing or its value is anything other than the two valid options.
    """
    try:
        kid: str = jwt.get_unverified_header(token)['kid']  # type: ignore[no-untyped-call]
    except KeyError:
        raise InvalidAuthData("JWT missing key ID header")
    try:
        kid = TokenIssuer(kid)
    except ValueError:
        raise InvalidAuthData(f"{kid} is not a valid key ID")
    return kid == TokenIssuer.internal


def check_and_save_jwt_data(request: Request, min_level: int, jwt_model: Type[RequestJWTData]) -> None:
    """
    Finds the JSON Web Token in a request and validates it before saving the decoded data back into the request.

    Args:
        request: The `aiohttp` request object to check and save to
        min_level: The minimum access level (see `Role`) the requesting party needs for the specified request.
        jwt_model: The JWT model class to use for decoding and parsing the JWT string.

    Raises:
        `InvalidAuthData` if the key ID header in the JWT was missing or invalid or the data object parsing failed.
        `NotAuthorized` if the role encoded in the JWT is lower than `min_role_level`.
    """
    token = extract_jwt_from_request(request)
    internal = min_level >= Role.node or jwt_kid_internal(token)
    try:
        data = jwt_model.decode(token, internal=internal)
    except jwt.InvalidTokenError as error:
        msg = f"Invalid JWT error: {error} ({request.url}); min. access level: {Role(min_level).name}"
        if min_level >= Role.lms:
            logger.info(msg)
        else:
            logger.debug(msg)
        raise error
    except ValidationError as error:
        raise InvalidAuthData(f"JWT data does not correspond to expected data: {error}")
    if data.role < min_level:
        raise NotAuthorized()
    request['jwt_data'] = data


def ensure_auth(min_level: int, *, headers: Optional[LooseHeaders] = None) -> Callable[[RouteHandler], RouteHandler]:
    """
    Decorator for route handler functions ensuring only authorized access to the decorated route.

    It checks that the request has a valid JWT and that its issuer has the role needed for the action.
    It does this by trying to match the JWT data with the JWT model in the type annotation of the decorated function.

    The decorated route handler function **must** be have exactly one parameter annotated with `RequestJWTData`
    (or a subclass) in its signature.

    It also checks that access to admin routes does not come from a reverse proxy, if the settings forbid this.

    Args:
        min_level: The minimum access level required for the decorated route.
        headers (optional): If an error, these additional headers can be sent along with the response.

    Returns:
        The internal decorator that wraps the actual route handler function.
    """
    min_level = Role(min_level)  # immediately throws a `ValueError` if `min_level` is not a valid `Role` value

    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        param_name, param_class = get_route_model_param(function, RequestJWTData)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            try:
                check_and_save_jwt_data(request, min_level, param_class)
            except (jwt.InvalidTokenError, NotAuthorized):
                raise HTTPUnauthorized(headers=headers)
            except InvalidAuthData as e:
                logger.info("Auth data invalid: %s", str(e))
                raise HTTPBadRequest(headers=headers)
            if min_level >= Role.admin and settings.forbid_admin_via_proxy and 'X-Forwarded-For' in request.headers:
                raise HTTPForbidden(headers=headers)
            kwargs[param_name] = request['jwt_data']
            return await function(request, *args, **kwargs)

        return cast(RouteHandler, wrapper)
    return decorator
