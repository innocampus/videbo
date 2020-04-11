from enum import Enum
from typing import Optional, Tuple, List

from livestreaming.web import JSONBaseModel


class DistributorStatus(JSONBaseModel):
    bound_to_storage_node_base_url: str
    tx_current_rate: int  # in Mbit/s
    tx_max_rate: int  # in Mbit/s
    rx_current_rate: int  # in Mbit/s
    tx_total: int  # in MB
    rx_total: int  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    space_free: int  # in MB


class DistributorCopyFile(JSONBaseModel):
    url: str


class DistributorDeleteFiles(JSONBaseModel):
    files: List[Tuple[str, str]]  # (hash, file extension)


class DistributorDeleteFilesResponse(JSONBaseModel):
    space_free: int  # in MB
