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
    free_space: int  # in MB


class DistributorCopyFile(JSONBaseModel):
    from_base_url: str
    file_size: int  # in bytes


class DistributorDeleteFiles(JSONBaseModel):
    files: List[Tuple[str, str]]  # (hash, file extension)


class DistributorDeleteFilesResponse(JSONBaseModel):
    free_space: int  # in MB


class DistributorFileList(JSONBaseModel):
    files: List[Tuple[str, str]]  # (hash, file extension)
