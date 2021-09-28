from enum import Enum
from typing import Optional, List, FrozenSet, Dict

from videbo.web import BaseJWTData, JSONBaseModel
from videbo.models import NodeStatus
from videbo.distributor.api.models import DistributorStatus


class FileType(Enum):
    VIDEO = "video"
    THUMBNAIL = "thumbnail"
    VIDEO_TEMP = "video_temp"
    THUMBNAIL_TEMP = "thumbnail_temp"

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return value in cls.values()

    @classmethod
    def values(cls) -> FrozenSet[str]:
        return frozenset(member.value for member in cls.__members__.values())


class UploadFileJWTData(BaseJWTData):
    is_allowed_to_upload_file: bool


class SaveFileJWTData(BaseJWTData):
    is_allowed_to_save_file: bool


class DeleteFileJWTData(BaseJWTData):
    is_allowed_to_delete_file: bool


class RequestFileJWTData(BaseJWTData):
    type: FileType
    hash: str
    file_ext: str
    thumb_id: Optional[int]
    rid: str  # random string identifying the user


class StorageStatus(NodeStatus):
    distributor_nodes: List[str]  # list of base_urls
    num_current_uploads: int


class StorageFileInfo(JSONBaseModel):
    hash: str
    file_extension: str
    file_size: int  # in MB

    def __repr__(self) -> str:
        return self.hash + self.file_extension

    def __str__(self) -> str:
        return repr(self)


class StorageFilesList(JSONBaseModel):
    files: List[StorageFileInfo]


class DistributorNodeInfo(JSONBaseModel):
    base_url: str


class DistributorStatusDict(JSONBaseModel):
    nodes: Dict[str, DistributorStatus]  # keys are base urls


class DeleteFilesList(JSONBaseModel):
    hashes: List[str]
