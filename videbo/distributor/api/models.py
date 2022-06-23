from typing import List

from videbo.web import JSONBaseModel
from videbo.models import NodeStatus, FileID


class DistributorCopyFileStatus(JSONBaseModel):
    hash: str
    file_ext: str
    loaded: int  # in bytes
    file_size: int  # in bytes
    duration: float  # in seconds


class DistributorStatus(NodeStatus):
    bound_to_storage_node_base_url: str
    waiting_clients: int  # number of clients waiting for a file being downloaded
    copy_files_status: List[DistributorCopyFileStatus]


class DistributorCopyFile(JSONBaseModel):
    from_base_url: str
    file_size: int  # in bytes


class DistributorDeleteFiles(JSONBaseModel):
    files: List[FileID]  # (hash, file extension)
    safe: bool  # if True, recently requested files will not be deleted


class DistributorDeleteFilesResponse(JSONBaseModel):
    files_skipped: List[FileID]  # (hash, file extension)
    free_space: float  # in MB


class DistributorFileList(JSONBaseModel):
    files: List[FileID]  # (hash, file extension)
