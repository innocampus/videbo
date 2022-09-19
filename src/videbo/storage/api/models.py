from enum import Enum
from typing import Optional

from videbo.models import BaseJWTData, BaseModel, BaseRequestModel, BaseResponseModel, RequestJWTData, NodeStatus
from videbo.distributor.api.models import DistributorStatus


__all__ = [
    'FileType',
    'UploadFileJWTData',
    'SaveFileJWTData',
    'DeleteFileJWTData',
    'RequestFileJWTData',
    'StorageStatus',
    'StorageFileInfo',
    'StorageFilesList',
    'DistributorNodeInfo',
    'DistributorStatusDict',
    'DeleteFilesList',
    'FileUploadedResponseJWT'
]


class FileType(Enum):
    VIDEO = 'video'
    THUMBNAIL = 'thumbnail'
    VIDEO_TEMP = 'video_temp'
    THUMBNAIL_TEMP = 'thumbnail_temp'

    @classmethod
    def values(cls) -> frozenset[str]:
        return frozenset(member.value for member in cls.__members__.values())


class UploadFileJWTData(RequestJWTData):
    is_allowed_to_upload_file: bool


class SaveFileJWTData(RequestJWTData):
    is_allowed_to_save_file: bool


class DeleteFileJWTData(RequestJWTData):
    is_allowed_to_delete_file: bool


class RequestFileJWTData(RequestJWTData):
    type: FileType
    hash: str
    file_ext: str
    thumb_id: Optional[int]
    rid: str  # random string identifying the user


class StorageStatus(NodeStatus):
    distributor_nodes: list[str]  # list of base_urls
    num_current_uploads: int


class StorageFileInfo(BaseModel):
    hash: str
    file_extension: str
    file_size: int  # in MB

    def __repr__(self) -> str:
        return self.hash + self.file_extension

    def __str__(self) -> str:
        return repr(self)

    class Config:
        orm_mode = True


class StorageFilesList(BaseResponseModel):
    files: list[StorageFileInfo]


class DistributorNodeInfo(BaseRequestModel):
    base_url: str


class DistributorStatusDict(BaseResponseModel):
    nodes: dict[str, DistributorStatus]  # keys are base urls


class DeleteFilesList(BaseRequestModel):
    hashes: list[str]


class FileUploadedResponseJWT(BaseJWTData):
    hash: str
    file_ext: str
    thumbnails_available: int
    duration: float
