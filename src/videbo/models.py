from enum import Enum, IntEnum
from time import time
from typing import Any, ClassVar, Optional, Type, TypeVar, Union

import jwt
from aiohttp.web import Response
from pydantic import BaseModel, validator

from videbo import storage_settings


__all__ = [
    'DEFAULT_JWT_ALG',
    'JSONBaseModel',
    'TokenIssuer',
    'Role',
    'BaseJWTData',
    'RequestJWTData',
    'LMSRequestJWTData',
    'VideoExistsRequest',
    'VideoExistsResponse',
    'NodeStatus',
]

J = TypeVar('J', bound='BaseJWTData')

DEFAULT_JWT_ALG = 'HS256'


class JSONBaseModel(BaseModel):
    def json_response(self, status_code: int = 200, **kwargs: Any) -> Response:
        return Response(text=self.json(**kwargs), status=status_code, content_type='application/json')


class TokenIssuer(str, Enum):
    internal = 'int'
    external = 'ext'


class BaseJWTData(BaseModel):
    """
    Base data fields that have to be stored in the JWT.

    Contains only the standard fields defined by RFC 7519 that we require for all tokens.

    Allows encoding an instance's data as a JWT string, as well as decoding a JWT string to an instance of the model.
    """
    exp: int  # expiration time claim
    iss: TokenIssuer  # issuer claim

    @validator('iss', pre=True)
    def iss_is_enum_member(cls, v: Union[TokenIssuer, str]) -> TokenIssuer:
        """Coerces the issuer value to the appropriate `TokenIssuer` enum member."""
        if isinstance(v, TokenIssuer):
            return v
        if isinstance(v, str):
            try:
                return TokenIssuer(v)
            except ValueError:
                try:
                    return TokenIssuer[v]
                except KeyError:
                    raise ValueError(f"Invalid issuer '{v}'")
        raise TypeError(f"{repr(v)} is not a valid issuer type")

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Replaces enum members with their values in the dictionary representation.

        This is makes the dictionary serializable (for JWT), but preserves the enum sugar in the model attribute,
        which would be lost, if we were to use the `use_enum_values` configuration option.
        """
        d = super().dict(*args, **kwargs)
        for attr_name, attr_value in d.items():
            if isinstance(attr_value, Enum):
                d[attr_name] = attr_value.value
        return d

    def encode(self, *, algorithm: str = DEFAULT_JWT_ALG) -> str:
        """
        Encodes its data in the form of a JWT string.

        Args:
            algorithm (optional):
                JWT signature algorithm to use for encoding; defaults to the constant `DEFAULT_JWT_ALG`.

        Returns:
            The JWT string containing the model instance's data.
        """
        if self.iss == TokenIssuer.internal:
            secret = storage_settings.internal_api_secret
        else:
            secret = storage_settings.external_api_secret
        return jwt.encode(self.dict(exclude_unset=True), secret, algorithm=algorithm, headers={'kid': self.iss.value})

    @classmethod
    def decode(cls: Type[J], encoded: str, *, internal: bool = False, algorithm: str = DEFAULT_JWT_ALG) -> J:
        """
        Decodes a JWT string returning the data as an instance of the calling model.

        Args:
            encoded:
                The JWT string
            internal (optional):
                If `True` the token is assumed to be encoded with the internal secret and issuer claim, probably coming
                from another node or the admin CLI; otherwise it is assumed to come from an external party (e.g. a LMS).
                `False` by default.
            algorithm (optional):
                JWT signature algorithm to assume for decoding; defaults to the constant `DEFAULT_JWT_ALG`.

        Returns:
            Instance of the calling class containing the data that was encoded in the JWT.
        """
        if internal:
            secret, issuer = storage_settings.internal_api_secret, TokenIssuer.internal
        else:
            secret, issuer = storage_settings.external_api_secret, TokenIssuer.external
        decoded = jwt.decode(encoded, secret, algorithms=[algorithm], issuer=issuer.value)
        return cls.parse_obj(decoded)


class Role(IntEnum):
    """All roles in a system ordered by powerfulness."""
    client = 0
    lms = 2
    node = 3
    admin = 5


class RequestJWTData(BaseJWTData):
    """
    Base model for the JWT data required in all authenticated routes.

    In addition to the base class, the role is added as a mandatory field.
    """
    role: Role

    @validator('role', pre=True)
    def role_is_enum_member(cls, v: Union[Role, int, str]) -> Role:
        """Coerces the role value to the appropriate `Role` enum member."""
        if isinstance(v, Role):
            return v
        if isinstance(v, int):
            return Role(v)
        if isinstance(v, str):
            try:
                return Role[v]
            except KeyError:
                raise ValueError(f"Invalid role name '{v}'")
        raise TypeError(f"{repr(v)} is not a valid role type")

    @validator('role')
    def role_appropriate(cls, v: Role, values: dict[str, Any]) -> Role:
        """Ensures that the role level is not greater than `lms`, if the issuer is supposed to be external."""
        if values.get('iss') == TokenIssuer.external and v > Role.lms:
            raise ValueError("External tokens can only be issued for a role up to `lms`")
        return v


class LMSRequestJWTData(RequestJWTData):
    # Cache to avoid encoding a new token for each request:
    _current_token: ClassVar[tuple[str, int]] = '', 0

    @validator('role')
    def role_appropriate(cls, v: Role, values: dict[str, Any]) -> Role:
        """Ensures that the role level is `node`."""
        if v != Role.node:
            raise ValueError("Tokens for accessing the LMS API must have the role `node`")
        return v

    @validator('iss')
    def only_external_issuer(cls, v: TokenIssuer) -> TokenIssuer:
        if v != TokenIssuer.external:
            raise ValueError("Tokens for accessing the LMS API must be `external`")
        return v

    @classmethod
    def get_standard_token(cls) -> str:
        current_time = int(time())
        if cls._current_token[1] < current_time:
            new_expiration = current_time + 4 * 3600
            data = cls(
                exp=new_expiration,
                iss=TokenIssuer.external,
                role=Role.node,
            )
            cls._current_token = data.encode(), new_expiration
        return cls._current_token[0]


class VideoExistsRequest(JSONBaseModel):
    hash: str
    file_ext: str


class VideoExistsResponse(JSONBaseModel):
    exists: bool


class NodeStatus(JSONBaseModel):
    tx_current_rate: float  # in Mbit/s
    tx_max_rate: float  # in Mbit/s
    rx_current_rate: float  # in Mbit/s
    tx_total: float  # in MB
    rx_total: float  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    files_total_size: float  # in MB
    files_count: int
    free_space: float  # in MB
