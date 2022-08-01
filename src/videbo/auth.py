import logging
import functools
from time import time
from typing import Any, Callable, Mapping, Optional, Tuple, Type, cast

import jwt
from aiohttp.typedefs import LooseHeaders
from aiohttp.web_request import Request
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized, HTTPForbidden
from pydantic import ValidationError

from videbo import storage_settings as settings
from videbo.exceptions import InvalidAuthData, NoJWTFound, NotAuthorized
from videbo.misc import get_route_model_param
from videbo.models import Role, BaseJWTData
from videbo.types import RouteHandler


logger = logging.getLogger('videbo-auth')

JWT_ALGORITHM = 'HS256'
JWT_ISS_EXTERNAL = 'ext'
JWT_ISS_INTERNAL = 'int'


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
        `NoJWTFound` if the assumptions mentioned above are not satisfied.
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
    logger.info("No JWT found in request.")
    raise NoJWTFound()


def jwt_kid_internal(token: str) -> bool:
    """
    Checks the key ID header of a JSON Web Token and determines if the token should be encoded as internal.

    Args:
        token: The JWT string to check

    Returns:
        `True` if the value of the "kid" header corresponds to `JWT_ISS_INTERNAL`.
        `False` if the value of the "kid" header corresponds to `JWT_ISS_EXTERNAL`.

    Raises:
        `InvalidAuthData` if the "kid" header is missing or its value is anything other than the two valid options.
    """
    try:
        kid: str = jwt.get_unverified_header(token)['kid']  # type: ignore[no-untyped-call]
    except KeyError:
        raise InvalidAuthData("JWT missing key ID header")
    if kid not in (JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL):
        raise InvalidAuthData(f"{kid} is not a valid key ID")
    return kid == JWT_ISS_INTERNAL


def _get_jwt_params(*, internal: bool) -> Tuple[str, str]:
    """Convenience function returning the correct API secret and JWT issuer claim for internal or external requests."""
    if internal:
        return settings.internal_api_secret, JWT_ISS_INTERNAL
    else:
        return settings.external_api_secret, JWT_ISS_EXTERNAL


def decode_jwt(encoded: str, *, model: Type[BaseJWTData] = BaseJWTData, internal: bool = False) -> BaseJWTData:
    """
    Decodes a JWT string returning the data in the form of the desired model.

    Args:
        encoded:
            The JWT string
        model (optional):
            The model the JWT data must correspond to. Defaults to `BaseJWTData`.
        internal (optional):
            If `True` the token is assumed to be encoded with the internal secret and issuer claim, probably coming
            from another node or the admin CLI; otherwise it is assumed to come from an external party (e.g. the LMS).
            `False` by default.

    Returns:
        Model object containing the data that was encoded in the JWT.
    """
    secret, issuer = _get_jwt_params(internal=internal)
    decoded = jwt.decode(encoded, secret, algorithms=[JWT_ALGORITHM], issuer=issuer)
    return model.parse_obj(decoded)


def encode_jwt(data: BaseJWTData, *, expiry: int = 300, internal: bool = False) -> str:
    """
    Encodes provided data in the form of a JWT string.

    Args:
        data:
            An instance of `BaseJWTData` (or a subclass) carrying the data to encode.
        expiry (optional):
            The time in seconds from the moment of the function call until the token is set to expire.
            Defaults to 300 (5 minutes).
        internal (optional):
            If `True` the token is encoded with the internal secret and issuer claim, likely for use with another node
            or the admin CLI; otherwise it is created for an external party (e.g. the LMS).

    Returns:
        The JWT string containing the provided data.
    """
    secret, issuer = _get_jwt_params(internal=internal)
    data.exp = int(time()) + expiry
    data.iss = issuer
    validated = data.__class__(**data.dict(exclude_unset=True))
    return jwt.encode(validated.dict(exclude_unset=True), secret, algorithm=JWT_ALGORITHM, headers={'kid': issuer})


# TODO: Implement this as a model validator.
def check_issuer_claim(data: BaseJWTData) -> None:
    """Validates the issuer claim in a JWT data object."""
    if data.iss == JWT_ISS_EXTERNAL and data.role > Role.lms:
        raise InvalidAuthData("External tokens can only be issued for a role up to LMS")
    if data.iss not in (JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL):
        raise InvalidAuthData(f"{data.iss} is not a valid issuer claim")


def check_and_save_jwt_data(request: Request, min_level: int, model: Type[BaseJWTData]) -> None:
    """
    Find the JSON Web Token in a request and validates it before saving the decoded data back into the request.

    Args:
        request: The `aiohttp` request object to check and save to
        min_level: The minimum access level (see `Role`) the requesting party needs for the specified request.
        model: The JWT model class to use for decoding and parsing the JWT string.

    Raises:
        `NoJWTFound` if no JWT could be found in the request.
        `InvalidAuthData` if the key ID header in the JWT was missing or invalid or the data object parsing failed.
        `NotAuthorized` if the role encoded in the JWT is lower than `min_role_level`.
    """
    token = extract_jwt_from_request(request)
    internal = min_level >= Role.node or jwt_kid_internal(token)
    try:
        data = decode_jwt(token, model=model, internal=internal)
    except jwt.InvalidTokenError as error:
        msg = f"Invalid JWT error: {error} ({request.url}); min. access level: {Role(min_level).name}"
        if min_level >= Role.lms:
            logger.info(msg)
        else:
            logger.debug(msg)
        raise error
    except ValidationError as error:
        logger.info(f"JWT data does not correspond to expected data: {error}")
        raise InvalidAuthData()
    check_issuer_claim(data)
    if data.role < min_level:
        raise NotAuthorized()
    request['jwt_data'] = data


def ensure_auth(min_level: int, headers: Optional[LooseHeaders] = None) -> Callable[[RouteHandler], RouteHandler]:
    """
    Decorator for route handler functions ensuring only authorized access to the decorated route.

    It checks that the request has a valid JWT, that the issuer has the role needed for the action.
    It does this by trying to match the JWT data with the JWT model in the type annotation of the decorated function.

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
        param_name, param_class = get_route_model_param(function, BaseJWTData)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            try:
                check_and_save_jwt_data(request, min_level, param_class)
            except (jwt.InvalidTokenError, NotAuthorized):
                raise HTTPUnauthorized(headers=headers)
            except InvalidAuthData:
                raise HTTPBadRequest(headers=headers)
            if min_level >= Role.admin and settings.forbid_admin_via_proxy and 'X-Forwarded-For' in request.headers:
                raise HTTPForbidden(headers=headers)
            kwargs[param_name] = request['jwt_data']
            return await function(request, *args, **kwargs)

        return cast(RouteHandler, wrapper)
    return decorator
