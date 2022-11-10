from __future__ import annotations
from enum import Enum
from typing import Optional

from videbo import settings
from videbo.models import BaseJWTData, BaseModel, BaseRequestModel, BaseResponseModel, RequestJWTData, NodeStatus, Role, TokenIssuer
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
    thumb_id: Optional[int] = None
    rid: str  # random string identifying the user

    @classmethod
    def client_default(
        cls,
        file_hash: str,
        file_ext: str,
        *,
        temp: bool,
        expiration_time: Optional[int] = None,
        thumb_id: Optional[int] = None,
    ) -> RequestFileJWTData:
        """
        Returns an instance with typical data for a client request.

        The `role` will always be `Role.client`, the `iss` field will be
        assigned `TokenIssuer.external`, and `rid` will be an empty string.

        Args:
            file_hash:
                Assigned to the `hash` field
            file_ext:
                Assigned to the `file_ext` field
            temp:
                If `True`, the `type` field is assigned `FileType.VIDEO_TEMP`
                or `FileType.THUMBNAIL_TEMP` depending on the value of the
                `thumb_id` argument; otherwise the `type` will be either
                `FileType.VIDEO` or `FileType.THUMBNAIL`.
            expiration_time (optional):
                If passed, the value will be assigned to the `exp` field;
                if omitted or `None` (default), the
                `default_expiration_from_now` method is called
                and its return value is assigned to `exp`.
            thumb_id (optional):
                If provided, will be assigned to the `thumb_id` field and
                `type` will be assigned either `FileType.THUMBNAIL_TEMP` or
                `FileType.THUMBNAIL` depending on the value of the `temp`
                argument; if omitted or `None` (default), `type` will be
                `FileType.VIDEO_TEMP` or `FileType.VIDEO` instead.

        Returns:
            Instance of the class with sane defaults for client requests.
        """
        if thumb_id is None:
            type_ = FileType.VIDEO_TEMP if temp else FileType.VIDEO
        else:
            type_ = FileType.THUMBNAIL_TEMP if temp else FileType.THUMBNAIL
        return cls(
            exp=expiration_time or cls.default_expiration_from_now(),
            iss=TokenIssuer.external,
            role=Role.client,
            type=type_,
            hash=file_hash,
            file_ext=file_ext,
            thumb_id=thumb_id,
            rid='',
        )

    def encode_public_file_url(self) -> str:
        """Returns the file url with the encoded JWT as a query parameter."""
        return f"{settings.public_base_url}/file?jwt={self.encode()}"


class StorageStatus(NodeStatus):
    distributor_nodes: list[str]  # list of base_urls
    num_current_uploads: int


class StorageFileInfo(BaseModel):
    hash: str
    file_ext: str
    file_size: int  # in MB

    def __repr__(self) -> str:
        return self.hash + self.file_ext

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
