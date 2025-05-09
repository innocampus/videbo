from videbo.models import (
    BaseRequestModel,
    BaseResponseModel,
    HashedFilesList,
    NodeStatus,
    VideboBaseModel,
)


__all__ = [
    'DistributorCopyFile',
    'DistributorCopyFileStatus',
    'DistributorDeleteFiles',
    'DistributorDeleteFilesResponse',
    'DistributorFileList',
    'DistributorStatus'
]


class DistributorCopyFileStatus(VideboBaseModel):
    hash: str
    file_ext: str
    loaded: int  # in bytes
    file_size: int  # in bytes
    duration: float  # in seconds


class DistributorStatus(NodeStatus):
    bound_to_storage_node_base_url: str
    waiting_clients: int  # number of clients waiting for a file being downloaded
    copy_files_status: list[DistributorCopyFileStatus]


# TODO(daniil-berg): Add `hash` and `file_ext` to this payload model.
#                    https://github.com/innocampus/videbo/issues/28
class DistributorCopyFile(BaseRequestModel):
    from_base_url: str
    file_size: int  # in bytes


class DistributorDeleteFiles(BaseRequestModel):
    files: HashedFilesList
    safe: bool  # if True, recently requested files will not be deleted


class DistributorDeleteFilesResponse(BaseResponseModel):
    files_skipped: HashedFilesList
    free_space: float  # in MB


class DistributorFileList(BaseResponseModel):
    files: HashedFilesList
