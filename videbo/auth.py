import logging
import functools
import inspect
from enum import IntEnum
from time import time
from typing import Any, Callable, Dict, Mapping, Optional, Type, Union, cast

import jwt
from aiohttp.typedefs import LooseHeaders
from aiohttp.web_request import Request
from aiohttp.web_exceptions import HTTPUnauthorized
from pydantic import BaseModel, ValidationError

from videbo import storage_settings as settings
from videbo.exceptions import InvalidRouteSignature, InvalidRoleIssued, NoJWTFound
from videbo.models import BaseJWTData
from videbo.types import RouteHandler


logger = logging.getLogger('videbo-auth')

JWT_ALGORITHM = 'HS256'
JWT_ISS_EXTERNAL = 'ext'
JWT_ISS_INTERNAL = 'int'


class Role(IntEnum):
    """All roles in a system ordered by powerfulness."""
    client = 0
    lms = 2
    node = 3
    admin = 5

    @classmethod
    def get_level(cls, role: str, issuer: str) -> int:
        """Get the level of a role and check if the issuer is allowed to use this role."""
        try:
            level = cls[role]
        except KeyError:
            raise InvalidRoleIssued()
        # External tokens can only be issued for a role up to lms.
        if (issuer == JWT_ISS_EXTERNAL and level > cls.lms) or issuer not in (JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL):
            raise InvalidRoleIssued()
        return level


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


# TODO: Refactor en-/decoding functions
def internal_jwt_decode(encoded: str) -> Dict[str, Any]:
    """Decode JSON Web token using the internal secret."""
    return jwt.decode(encoded, settings.internal_api_secret, algorithms=[JWT_ALGORITHM], issuer=JWT_ISS_INTERNAL)


def internal_jwt_encode(data: Union[Dict[str, Any], BaseJWTData], expiry: int = 300) -> str:
    """Encode data to JSON Web token using the internal secret."""
    if isinstance(data, dict):
        data['exp'] = int(time()) + expiry
        data['iss'] = JWT_ISS_INTERNAL
    elif isinstance(data, BaseJWTData):
        data.exp = int(time()) + expiry
        data.iss = JWT_ISS_INTERNAL
        data = data.dict()

    headers = {
        'kid': JWT_ISS_INTERNAL
    }
    return jwt.encode(data, settings.internal_api_secret, algorithm=JWT_ALGORITHM, headers=headers)


def external_jwt_decode(encoded: str) -> Dict[str, Any]:
    """Decode JSON Web token using the external/lms secret."""
    return jwt.decode(encoded, settings.external_api_secret, algorithms=[JWT_ALGORITHM], issuer=JWT_ISS_EXTERNAL)


def external_jwt_encode(data: Union[Dict[str, Any], BaseJWTData], expiry: int = 300) -> str:
    """Encode data to JSON Web token using the external secret."""
    if isinstance(data, dict):
        data['exp'] = int(time()) + expiry
        data['iss'] = JWT_ISS_EXTERNAL
    elif isinstance(data, BaseJWTData):
        data.exp = int(time()) + expiry
        data.iss = JWT_ISS_EXTERNAL
        data = data.dict()

    headers = {
        'kid': JWT_ISS_EXTERNAL
    }
    return jwt.encode(data, settings.external_api_secret, algorithm=JWT_ALGORITHM, headers=headers)


def check_jwt_auth_save_data(request: Request, min_role_level: int, model: Type[BaseJWTData]) -> bool:
    """Check user's authentication by looking at the JWT and the given role and save the JWT data in request.

    It also considers if the jwt was generated internally or externally."""
    try:
        token = extract_jwt_from_request(request)

        if min_role_level >= Role.node:
            # the issuer must be internal anyway
            decoded = internal_jwt_decode(token)
        else:
            # check the header for the kid field
            kid = jwt.get_unverified_header(token)['kid']  # type: ignore[no-untyped-call]
            if kid == JWT_ISS_EXTERNAL:
                decoded = external_jwt_decode(token)
            elif kid == JWT_ISS_INTERNAL:
                decoded = internal_jwt_decode(token)
            else:
                return False

        try:
            data = model.parse_obj(decoded)
        except ValidationError as error:
            logger.info(f"JWT data does not correspond to expected data: {error}")
            return False

        request['jwt_data'] = data
        return Role.get_level(decoded['role'], decoded['iss']) >= min_role_level

    except KeyError:
        pass
    except NoJWTFound:
        logger.info('No JWT found in request.')
    except jwt.InvalidTokenError as error:
        msg = f"Invalid JWT error: {error} ({request.url}), min role level {min_role_level}"
        if min_role_level >= Role.lms:
            logger.info(msg)
        else:
            logger.debug(msg)
    except InvalidRoleIssued:
        logger.info('JWT has a role that the issuer is not allowed to take on')

    return False


def ensure_jwt_data_and_role(min_role_level: int,
                             headers: Optional[LooseHeaders] = None) -> Callable[[RouteHandler], RouteHandler]:
    """Decorator function used to ensure that there is a valid JWT.

    It checks that the request has the role needed for the action and tries to match the jwt data with the model
    given as a type annotation in func.

    On an error, headers can be sent along the response.
    """
    # TODO: Refactor together with `web.ensure_json_body`
    def decorator(function: RouteHandler) -> RouteHandler:
        """internal decorator function"""
        # Look for the JWT data model given in a type annotation.
        signature = inspect.signature(function)
        param: inspect.Parameter
        jwt_data_model_arg_name: Optional[str] = None
        jwt_data_model_arg_model: Type[BaseModel] = BaseModel
        for name, param in signature.parameters.items():
            if issubclass(param.annotation, BaseJWTData):
                if jwt_data_model_arg_name:
                    raise InvalidRouteSignature(f"More than one parameter of the type `{BaseJWTData.__name__}` "
                                                f"present in function `{function.__name__}`.")
                jwt_data_model_arg_name = name
                jwt_data_model_arg_model = param.annotation
        if jwt_data_model_arg_name is None:
            raise InvalidRouteSignature(f"No parameter of the type `{BaseJWTData.__name__}` present in function "
                                        f"`{function.__name__}`.")

        @functools.wraps(function)
        async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
            """Wrapper around the actual function call."""
            assert issubclass(jwt_data_model_arg_model, BaseJWTData) and isinstance(jwt_data_model_arg_name, str)
            if not check_jwt_auth_save_data(request, min_role_level, jwt_data_model_arg_model):
                raise HTTPUnauthorized(headers=headers)
            kwargs[jwt_data_model_arg_name] = request['jwt_data']
            return await function(request, *args, **kwargs)

        return cast(RouteHandler, wrapper)
    return decorator
