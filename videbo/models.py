from typing import Optional

from pydantic import BaseModel


class JSONBaseModel(BaseModel):
    pass


class BaseJWTData(BaseModel):
    """Base data fields that have to be stored in the JWT."""
    # standard fields defined by RFC 7519 that we require for all tokens
    exp: int  # expiration time claim
    iss: str  # issuer claim

    # role must always be present
    role: str


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
