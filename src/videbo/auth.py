import logging
import functools
from time import time
from typing import Any, Callable, Dict, Mapping, Optional, Tuple, Type, cast

import jwt
from aiohttp.typedefs import LooseHeaders
from aiohttp.web_request import Request
from aiohttp.web_exceptions import HTTPUnauthorized, HTTPForbidden
from pydantic import ValidationError

from videbo import storage_settings as settings
from videbo.exceptions import InvalidRoleIssued, InvalidIssuerClaim, NoJWTFound
from videbo.misc import get_route_model_param
from videbo.models import Role, BaseJWTData
from videbo.types import RouteHandler


logger = logging.getLogger('videbo-auth')

JWT_ALGORITHM = 'HS256'
JWT_ISS_EXTERNAL = 'ext'
JWT_ISS_INTERNAL = 'int'


def extract_jwt_from_request(request: Request) -> str:
    """Get the JSON Web Token from the Authorization header or GET field named jwt."""
    try:
        # First try to find the token in the header.
        prefix = 'Bearer '
        header_auth: str = request.headers.getone('Authorization')
        if header_auth.startswith(prefix):
            return header_auth[len(prefix):]
        raise NoJWTFound()
    except KeyError:
        pass
    # Then try to find the token in the GET field jwt.
    query: Mapping[str, str] = request.query
    if 'jwt' in query:
        return query['jwt']
    raise NoJWTFound()


def _get_jwt_params(*, internal: bool) -> Tuple[str, str]:
    """Convenience function returning the correct API secret and JWT issuer claim for internal or external requests."""
    if internal:
        return settings.internal_api_secret, JWT_ISS_INTERNAL
    else:
        return settings.external_api_secret, JWT_ISS_EXTERNAL


def decode_jwt(encoded: str, *, internal: bool = False) -> Dict[str, Any]:
    """
    Decodes a JWT string returning the data as a dictionary.

    Args:
        encoded:
            The JWT string
        internal (optional):
            If `True` the token is assumed to be encoded with the internal secret and issuer claim, probably coming
            from another node or the admin CLI; otherwise it is assumed to come from an external party (e.g. the LMS).

    Returns:
        Dictionary containing the data that was encoded in the JWT.
    """
    secret, issuer = _get_jwt_params(internal=internal)
    return jwt.decode(encoded, secret, algorithms=[JWT_ALGORITHM], issuer=issuer)


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


def check_issuer_claim(data: BaseJWTData) -> None:
    if data.iss == JWT_ISS_EXTERNAL and data.role > Role.lms:
        raise InvalidRoleIssued("External tokens can only be issued for a role up to LMS")
    if data.iss not in (JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL):
        raise InvalidIssuerClaim(f"{data.iss} is not a valid issuer claim")


def check_jwt_auth_save_data(request: Request, min_role_level: int, model: Type[BaseJWTData]) -> bool:
    """Check user's authentication by looking at the JWT and the given role and save the JWT data in request.

    It also considers if the jwt was generated internally or externally."""
    try:
        token = extract_jwt_from_request(request)

        if min_role_level >= Role.node:
            # the issuer must be internal anyway
            decoded = decode_jwt(token, internal=True)
        else:
            # check the header for the kid field
            kid = jwt.get_unverified_header(token)['kid']  # type: ignore[no-untyped-call]
            if kid == JWT_ISS_EXTERNAL:
                decoded = decode_jwt(token)
            elif kid == JWT_ISS_INTERNAL:
                decoded = decode_jwt(token, internal=True)
            else:
                return False

        # TODO: This should cause a Bad Request 400
        try:
            data = model.parse_obj(decoded)
        except ValidationError as error:
            logger.info(f"JWT data does not correspond to expected data: {error}")
            return False

        request['jwt_data'] = data
        check_issuer_claim(data)
        return data.role >= min_role_level

    except KeyError:
        pass
    except NoJWTFound:
        logger.info("No JWT found in request.")
    except jwt.InvalidTokenError as error:
        msg = f"Invalid JWT error: {error} ({request.url}), min role level {min_role_level}"
        if min_role_level >= Role.lms:
            logger.info(msg)
        else:
            logger.debug(msg)
    # TODO: Invalid issuer should cause 400
    except (InvalidRoleIssued, InvalidIssuerClaim):
        logger.info("JWT has a role that the issuer is not allowed to take on")

    return False


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
    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        param_name, param_class = get_route_model_param(function, BaseJWTData)

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            if not check_jwt_auth_save_data(request, min_level, param_class):
                raise HTTPUnauthorized(headers=headers)
            if min_level >= Role.admin and settings.forbid_admin_via_proxy and 'X-Forwarded-For' in request.headers:
                raise HTTPForbidden(headers=headers)
            kwargs[param_name] = request['jwt_data']
            return await function(request, *args, **kwargs)

        return cast(RouteHandler, wrapper)
    return decorator
