import functools
import inspect
import pydantic
import jwt
import logging
from aiohttp import web
from aiohttp.web_exceptions import HTTPUnauthorized
from time import time
from typing import Dict, Type, Optional, Union

from videbo import settings


logger = logging.getLogger('videbo-auth')
JWT_ALGORITHM = 'HS256'
JWT_ISS_EXTERNAL = 'ext'
JWT_ISS_INTERNAL = 'int'


class Role:
    """All roles in a system ordered by powerfulness."""
    client = 0
    streamer = 1
    lms = 2
    node = 3  # other node than manager
    manager = 4
    admin = 5

    @classmethod
    def get_level(cls, role: str, issuer: str) -> int:
        """Get the level of a role and check if the issuer is allowed to use this role."""
        level = getattr(cls, role)

        # External tokens can only be issued for a role up to lms.
        if (issuer == JWT_ISS_EXTERNAL and level > cls.lms) or issuer not in (JWT_ISS_EXTERNAL, JWT_ISS_INTERNAL):
            raise InvalidRoleIssuedError()

        return level


def extract_jwt_from_request(request: web.Request) -> str:
    """Get the JSON Web Token from the Authorization header or GET field named jwt."""
    try:
        # First try to find the token in the header.
        prefix = 'Bearer '
        header_auth = request.headers.getone('Authorization')
        if header_auth.startswith(prefix):
            return header_auth[len(prefix):]
        raise NoJWTFoundError()
    except KeyError:
        pass

    # Then try to find the token in the GET field jwt.
    query = request.query
    if 'jwt' in query:
        return query['jwt']
    raise NoJWTFoundError()


def internal_jwt_decode(encoded: str) -> Dict:
    """Decode JSON Web token using the internal secret."""
    return jwt.decode(encoded, settings.general.internal_api_secret, algorithms=JWT_ALGORITHM, issuer=JWT_ISS_INTERNAL)


def internal_jwt_encode(data: Union[Dict, 'BaseJWTData'], expiry: int = 300) -> str:
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
    return jwt.encode(data, settings.general.internal_api_secret, algorithm=JWT_ALGORITHM, headers=headers).decode('ascii')


def external_jwt_decode(encoded: str) -> Dict:
    """Decode JSON Web token using the external/lms secret."""
    return jwt.decode(encoded, settings.lms.api_secret, algorithms=JWT_ALGORITHM, issuer=JWT_ISS_EXTERNAL)


def external_jwt_encode(data: Dict, expiry: int = 300) -> str:
    """Encode data to JSON Web token using the internal secret."""
    data['exp'] = int(time()) + expiry
    data['iss'] = JWT_ISS_EXTERNAL
    headers = {
        'kid': JWT_ISS_EXTERNAL
    }
    return jwt.encode(data, settings.lms.api_secret, algorithm=JWT_ALGORITHM, headers=headers).decode('ascii')


def check_jwt_auth_save_data(request: web.Request, min_role_level: int, model: Type['BaseJWTData']) -> bool:
    """Check user's authentication by looking at the JWT and the given role and save the JWT data in request.

    It also considers if the jwt was generated internally or externally."""
    try:
        token = extract_jwt_from_request(request)

        if min_role_level >= Role.node:
            # the issuer must be internal anyway
            decoded = internal_jwt_decode(token)
        else:
            # check the header for the kid field
            kid = jwt.get_unverified_header(token)['kid']
            if kid == JWT_ISS_EXTERNAL:
                decoded = external_jwt_decode(token)
            elif kid == JWT_ISS_INTERNAL:
                decoded = internal_jwt_decode(token)
            else:
                return False

        try:
            data = match_jwt_with_model(decoded, model)
        except JWTDoesNotMatchModelError as error:
            logger.info('JWT data does not correspond to expected data: ' + str(error.pydantic_error))
            return False

        request['jwt_data'] = data
        return Role.get_level(decoded['role'], decoded['iss']) >= min_role_level

    except KeyError:
        pass
    except NoJWTFoundError:
        logger.info('No JWT found in request.')
    except jwt.InvalidTokenError as error:
        msg = f"Invalid JWT error: {error} ({request.url}), min role level {min_role_level}"
        if min_role_level >= Role.lms:
            logger.info(msg)
        else:
            logger.debug(msg)
    except InvalidRoleIssuedError:
        logger.info('JWT has a role that the issuer is not allowed to take on')

    return False


def match_jwt_with_model(data: Dict, model: Type['BaseJWTData']):
    """Try to match the jwt data with a model."""
    try:
        return model.parse_obj(data)
    except pydantic.ValidationError as error:
        raise JWTDoesNotMatchModelError(error)


def ensure_jwt_data_and_role(min_role_level: int, headers: Optional[dict] = None):
    """Decorator function used to ensure that there is a valid JWT.

    It checks that the request has the role needed for the action and tries to match the jwt data with the model
    given as a type annotation in func.

    On an error, headers can be sent along the response.
    """
    def decorator(func):
        """internal decorator function"""

        # Look for the JWt data model given in a type annotation.
        signature = inspect.signature(func)
        param: inspect.Parameter
        jwt_data_model_arg_name = None
        jwt_data_model_arg_model = None
        for name, param in signature.parameters.items():
            if issubclass(param.annotation, BaseJWTData):
                if jwt_data_model_arg_name:
                    raise TooManyJWTModelsError()
                jwt_data_model_arg_name = name
                jwt_data_model_arg_model = param.annotation

        if jwt_data_model_arg_name is None:
            raise NoJWTModelFoundError()

        @functools.wraps(func)
        async def wrapper(request: web.Request, *args, **kwargs):
            """Wrapper around the actual function call."""

            if not check_jwt_auth_save_data(request, min_role_level, jwt_data_model_arg_model):
                raise HTTPUnauthorized(headers=headers)

            kwargs[jwt_data_model_arg_name] = request['jwt_data']
            return await func(request, *args, **kwargs)

        return wrapper
    return decorator


class BaseJWTData(pydantic.BaseModel):
    """Base data fields that have to be stored in the JWT."""
    # standard fields defined by RFC 7519 that we require for all tokens
    exp: int # expiration time claim
    iss: str # issuer claim

    # role must always be present
    role: str


# Exceptions
class InvalidRoleIssuedError(Exception):
    pass


class NoJWTFoundError(Exception):
    pass


class JWTDoesNotMatchModelError(Exception):
    def __init__(self, error: pydantic.ValidationError):
        self.pydantic_error = error
        super().__init__()


class NoJWTModelFoundError(Exception):
    def __init__(self):
        super().__init__('Could not find a jwt model in the function as a parameter.')


class TooManyJWTModelsError(Exception):
    pass
