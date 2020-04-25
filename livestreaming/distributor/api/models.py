from enum import Enum
from typing import Optional, Tuple, List

from livestreaming.web import JSONBaseModel


class DistributorCopyFileStatus(JSONBaseModel):
    hash: str
    file_ext: str
    loaded: int  # in bytes
    file_size: int  # in bytes
    duration: float  # in seconds


class DistributorStatus(JSONBaseModel):
    bound_to_storage_node_base_url: str
    tx_current_rate: int  # in Mbit/s
    tx_max_rate: int  # in Mbit/s
    rx_current_rate: int  # in Mbit/s
    tx_total: int  # in MB
    rx_total: int  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    waiting_clients: int  # number of clients waiting for a file being downloaded
    files_total_size: int  # in MB
    files_count: int
    free_space: int  # in MB
    copy_files_status: List[DistributorCopyFileStatus]


class DistributorCopyFile(JSONBaseModel):
    from_base_url: str
    file_size: int  # in bytes


class DistributorDeleteFiles(JSONBaseModel):
    files: List[Tuple[str, str]]  # (hash, file extension)


class DistributorDeleteFilesResponse(JSONBaseModel):
    free_space: int  # in MB


class DistributorFileList(JSONBaseModel):
    files: List[Tuple[str, str]]  # (hash, file extension)
