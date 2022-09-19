from videbo.models import BaseModel, BaseRequestModel, BaseResponseModel, NodeStatus
from videbo.types import FileID


__all__ = [
    'DistributorCopyFileStatus',
    'DistributorStatus',
    'DistributorCopyFile',
    'DistributorDeleteFiles',
    'DistributorDeleteFilesResponse',
    'DistributorFileList'
]


class DistributorCopyFileStatus(BaseModel):
    hash: str
    file_ext: str
    loaded: int  # in bytes
    file_size: int  # in bytes
    duration: float  # in seconds


class DistributorStatus(NodeStatus):
    bound_to_storage_node_base_url: str
    waiting_clients: int  # number of clients waiting for a file being downloaded
    copy_files_status: list[DistributorCopyFileStatus]


class DistributorCopyFile(BaseRequestModel):
    from_base_url: str
    file_size: int  # in bytes


class DistributorDeleteFiles(BaseRequestModel):
    files: list[FileID]  # (hash, file extension)
    safe: bool  # if True, recently requested files will not be deleted


class DistributorDeleteFilesResponse(BaseResponseModel):
    files_skipped: list[FileID]  # (hash, file extension)
    free_space: float  # in MB


class DistributorFileList(BaseResponseModel):
    files: list[FileID]  # (hash, file extension)
