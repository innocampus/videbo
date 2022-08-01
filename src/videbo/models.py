from enum import Enum, IntEnum
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, validator


class JSONBaseModel(BaseModel):
    pass


class Role(IntEnum):
    """All roles in a system ordered by powerfulness."""
    client = 0
    lms = 2
    node = 3
    admin = 5


class BaseJWTData(BaseModel):
    """Base data fields that have to be stored in the JWT."""
    # standard fields defined by RFC 7519 that we require for all tokens
    exp: int  # expiration time claim
    iss: str  # issuer claim

    # role must always be present
    role: Role

    @validator('role', pre=True)
    def role_is_enum_member(cls, v: Union[Role, int, str]) -> Role:
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

    def dict(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
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
