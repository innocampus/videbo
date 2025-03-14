from __future__ import annotations
from enum import Enum, IntEnum
from logging import Logger, getLogger
from time import time
from typing import Any, ClassVar, Optional, TypeVar, Union

import jwt
from aiohttp.web import Response
from pydantic import BaseModel as PydanticBaseModel, validator

from videbo import __version__, settings


__all__ = [
    'DEFAULT_JWT_ALG',
    'BaseJWTData',
    'BaseRequestModel',
    'BaseResponseModel',
    'HashedFileModel',
    'HashedFilesList',
    'LMSRequestJWTData',
    'NodeStatus',
    'RequestJWTData',
    'Role',
    'TokenIssuer',
    'VideboBaseModel',
    'VideosMissingRequest',
    'VideosMissingResponse',
]

J = TypeVar('J', bound='BaseJWTData')

DEFAULT_JWT_ALG = 'HS256'

_log = getLogger(__name__)


class VideboBaseModel(PydanticBaseModel):
    class Config:
        orm_mode = True


class BaseRequestModel(VideboBaseModel):
    pass


class BaseResponseModel(VideboBaseModel):
    _status_code: int = 200

    def _log_response(self, log: Logger) -> None:
        pass

    def json_response(
        self,
        status_code: Optional[int] = None,
        log: Logger = _log,
        **kwargs: Any,
    ) -> Response:
        self._log_response(log)
        return Response(
            text=self.json(**kwargs),
            status=status_code or self._status_code,
            content_type="application/json",
        )

    class Config:
        underscore_attrs_are_private = True


class TokenIssuer(str, Enum):
    internal = 'int'
    external = 'ext'


class BaseJWTData(VideboBaseModel):
    """
    Base data fields that have to be stored in the JWT.

    Contains only the standard fields defined by RFC 7519 that we require for all tokens.

    Allows encoding an instance's data as a JWT string, as well as decoding a JWT string to an instance of the model.
    """
    DEFAULT_LIFE_TIME: ClassVar[float] = 60 * 60.0  # seconds

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
                    raise ValueError(f"Invalid issuer '{v}'") from None
        raise TypeError(f"{v!r} is not a valid issuer type")

    @property
    def internal(self) -> bool:
        return self.iss == TokenIssuer.internal

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Replaces enum members with their values in the dictionary representation.

        This makes the dictionary serializable (for JWT), but preserves the enum sugar in the model attribute,
        which would be lost, if we were to use the `use_enum_values` configuration option.
        """
        d = super().dict(*args, **kwargs)
        for attr_name, attr_value in d.items():
            if isinstance(attr_value, Enum):
                d[attr_name] = attr_value.value
        return d

    def encode(
        self,
        *,
        key: Optional[str] = None,
        algorithm: str = DEFAULT_JWT_ALG,
    ) -> str:
        """
        Encodes its data in the form of a JWT string.

        Args:
            key (optional):
                Key to use for signing the token; defaults to the
                internal/external secret from the global settings
                depending on the issuer.
            algorithm (optional):
                JWT signature algorithm to use for encoding;
                defaults to the constant `DEFAULT_JWT_ALG`.

        Returns:
            The JWT string containing the model instance's data.
        """
        if key is None:
            if self.internal:
                key = settings.internal_api_secret
            else:
                key = settings.external_api_secret
        return jwt.encode(
            self.dict(exclude_unset=True),
            key,
            algorithm=algorithm,
            headers={'kid': self.iss.value},
        )

    @classmethod
    def decode(
        cls: type[J],
        encoded: str,
        *,
        internal: bool = False,
        key: Optional[str] = None,
        algorithm: str = DEFAULT_JWT_ALG,
    ) -> J:
        """
        Decodes a JWT string returning the data as an instance of the calling model.

        Args:
            encoded:
                The JWT string
            internal (optional):
                If `True` the token is assumed to be encoded with the
                internal secret and issuer claim, probably coming from
                another node or the admin CLI; if `False` (default) it is
                assumed to come from an external party (e.g. a LMS).
                If a `key` argument is provided, the issuer is set to `None`
                and that argument is used to decode the token.
            key (optional):
                Key to use for signing the token; defaults to the
                internal/external secret from the global settings depending
                on the issuer. If provided, overrides `internal`.
            algorithm (optional):
                JWT signature algorithm to assume for decoding;
                defaults to the constant `DEFAULT_JWT_ALG`.

        Returns:
            Instance of the calling class containing the data that was encoded in the JWT.
        """
        issuer = None
        if key is None:
            if internal:
                key = settings.internal_api_secret
                issuer = TokenIssuer.internal.value
            else:
                key = settings.external_api_secret
                issuer = TokenIssuer.external.value
        decoded = jwt.decode(
            encoded,
            key,
            algorithms=[algorithm],
            issuer=issuer,
        )
        return cls.parse_obj(decoded)

    @classmethod
    def default_expiration_from_now(cls) -> int:
        """Returns the timestamp `DEFAULT_LIFE_TIME` seconds from now."""
        return int(time() + cls.DEFAULT_LIFE_TIME)


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
                raise ValueError(f"Invalid role name '{v}'") from None
        raise TypeError(f"{v!r} is not a valid role type")

    @validator('role')
    def role_appropriate(cls, v: Role, values: dict[str, Any]) -> Role:
        """Ensures that the role level is not greater than `lms`, if the issuer is supposed to be external."""
        if values.get('iss') == TokenIssuer.external and v > Role.lms:
            raise ValueError("External tokens can only be issued for a role up to `lms`")
        return v


class LMSRequestJWTData(RequestJWTData):
    # Cache to avoid encoding a new token for each request:
    _current_token: ClassVar[tuple[str, int]] = '', 0

    iss: TokenIssuer = TokenIssuer.external
    role: Role = Role.node

    @validator('role')
    def role_appropriate(cls, v: Role, values: dict[str, Any]) -> Role:  # noqa: ARG003
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


class HashedFileModel(VideboBaseModel):
    hash: str
    ext: str

    def __repr__(self) -> str:
        return self.hash + self.ext

    def __str__(self) -> str:
        return repr(self)


HashedFilesList = list[HashedFileModel]


class VideosMissingRequest(BaseRequestModel):
    hashes: list[str]

    @validator("hashes")
    def at_least_one_video(cls, v: HashedFilesList) -> HashedFilesList:
        if len(v) == 0:
            raise ValueError("Hashes list must contain at least one video")
        return v


class VideosMissingResponse(BaseResponseModel):
    hashes: list[str]  # i.e. not known/managed by the LMS


class NodeStatus(BaseResponseModel):
    version: str = __version__
    tx_current_rate: float  # in Mbit/s
    rx_current_rate: float  # in Mbit/s
    tx_total: float  # in MB
    rx_total: float  # in MB
    tx_max_rate: float  # in Mbit/s
    current_connections: Optional[int]  # HTTP connections serving videos
    files_total_size: float  # in MB
    files_count: int
    free_space: float  # in MB

    class Config:
        validate_assignment = True
