from __future__ import annotations
from enum import Enum
from logging import Logger, getLogger
from typing import Literal, Optional

from pydantic.fields import Field
from pydantic.networks import AnyHttpUrl  # noqa: TCH002

from videbo import settings
from videbo.models import (
    BaseJWTData,
    BaseRequestModel,
    BaseResponseModel,
    HashedFileModel,
    NodeStatus,
    RequestJWTData,
    Role,
    TokenIssuer,
)
from videbo.distributor.api.models import DistributorStatus  # noqa: TCH001


__all__ = [
    'FileType',
    'Status',
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
    'FileUploadedResponseJWT',
    'OK',
    'MaxSizeMB',
    'FileTooBig',
    'InvalidFormat',
    'FileUploaded',
    'FileDoesNotExist',
    'NotAllFilesDeleted',
]

_log = getLogger(__name__)


# TODO(daniil-berg): Move to `videbo.models`.
#                    https://github.com/innocampus/videbo/issues/22
class FileType(Enum):
    VIDEO = 'video'
    THUMBNAIL = 'thumbnail'
    VIDEO_TEMP = 'video_temp'
    THUMBNAIL_TEMP = 'thumbnail_temp'

    @classmethod
    def values(cls) -> frozenset[str]:
        return frozenset(member.value for member in cls.__members__.values())


class Status(str, Enum):
    OK = "ok"
    ERROR = "error"
    INCOMPLETE = "incomplete"


#######################
# JWT request models: #

class UploadFileJWTData(RequestJWTData):
    is_allowed_to_upload_file: bool


class SaveFileJWTData(RequestJWTData):
    is_allowed_to_save_file: bool


class DeleteFileJWTData(RequestJWTData):
    is_allowed_to_delete_file: bool


# TODO(daniil-berg): Move to `videbo.models`.
#                    https://github.com/innocampus/videbo/issues/22
class RequestFileJWTData(RequestJWTData):
    type: FileType
    hash: str
    file_ext: str
    thumb_id: Optional[int] = None
    rid: str  # random string identifying the user

    @classmethod
    def node_default(
        cls,
        file_hash: str,
        file_ext: str,
        *,
        expiration_time: Optional[int] = None,
    ) -> RequestFileJWTData:
        """
        Returns an instance with typical data for an inter-node request.

        The `type` will always `FileType.VIDEO`, the `role` will be `Role.node`,
        the `iss` field will be assigned `TokenIssuer.internal`, and `rid` will
        be an empty string.

        Args:
            file_hash:
                Assigned to the `hash` field
            file_ext:
                Assigned to the `file_ext` field
            expiration_time (optional):
                If passed, the value will be assigned to the `exp` field;
                if omitted or `None` (default), the
                `default_expiration_from_now` method is called
                and its return value is assigned to `exp`.

        Returns:
            Instance of the class with sane defaults for inter-node requests.
        """
        return cls(
            exp=expiration_time or cls.default_expiration_from_now(),
            iss=TokenIssuer.internal,
            role=Role.node,
            type=FileType.VIDEO,
            hash=file_hash,
            file_ext=file_ext,
            thumb_id=None,
            rid='',
        )

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

    @classmethod
    def get_urls(
        cls,
        file_hash: str,
        file_ext: str,
        *,
        temp: bool,
        thumb_count: int,
        exp: Optional[int] = None,
    ) -> tuple[str, list[str]]:
        """
        Constructs the URLs for requesting a video file and its thumbnails.

        Each URL is created via the `encode_public_file_url` method.

        Args:
            file_hash:
                The hash hexdigest of the video file in question
            file_ext:
                The extension of the video file in question
            temp:
                If `True`, the video is assumed to not be saved permanently yet,
                but only recently uploaded.
            thumb_count:
                The number of thumbnails that can be requested for the video
            exp (optional):
                If provided, it will be set as the value for the `exp` field
                on the JWT model; otherwise the default expiration time is used.

        Returns:
            2-tuple with the first element being the URL by which to request
            the video and the second being a list of the thumbnail URLs
        """
        exp = exp or cls.default_expiration_from_now()
        video_url = cls.client_default(
            file_hash,
            file_ext,
            temp=temp,
            expiration_time=exp,
        ).encode_public_file_url()
        thumbnail_urls = []
        for thumb_id in range(thumb_count):
            thumbnail_urls.append(
                cls.client_default(
                    file_hash,
                    file_ext,
                    temp=temp,
                    expiration_time=exp,
                    thumb_id=thumb_id,
                ).encode_public_file_url()
            )
        return video_url, thumbnail_urls


########################
# JWT response models: #

class FileUploadedResponseJWT(BaseJWTData):
    hash: str
    file_ext: str
    thumbnails_available: int
    duration: float


#########################
# Response body models: #


class StatusResponseModel(BaseResponseModel):
    status: Status


class OK(StatusResponseModel):
    status: Status = Status.OK


class _Error(StatusResponseModel):
    status: Status = Status.ERROR


class _Incomplete(StatusResponseModel):
    status: Status = Status.INCOMPLETE


class MaxSizeMB(BaseResponseModel):
    max_size: float = Field(default_factory=lambda: settings.video.max_file_size_mb)


class FileTooBig(_Error, MaxSizeMB):
    _status_code: int = 413

    def _log_response(self, log: Logger) -> None:
        log.warning("File too big to upload")


class InvalidFormat(_Error):
    _status_code: int = 415
    error: Literal["invalid_format"] = "invalid_format"

    def _log_response(self, log: Logger) -> None:
        log.warning("No or invalid file in request")


# TODO(daniil-berg): Inherit from `OK`, remove `result` field.
#                    https://github.com/innocampus/videbo/issues/21
class FileUploaded(BaseResponseModel):
    result: Literal["ok"] = "ok"
    jwt: str
    url: AnyHttpUrl
    thumbnails: list[AnyHttpUrl]

    @classmethod
    def from_video(
        cls,
        file_hash: str,
        file_ext: str,
        *,
        thumbnails_available: int,
        duration: float,
    ) -> FileUploaded:
        """
        Returns an instance with data for the client after successful upload.

        The JWT will be an encoded instance of `FileUploadedResponseJWT`, for
        which the `iss` field will be assigned `TokenIssuer.external` and the
        return value of `default_expiration_from_now` is assigned to `exp`.

        Args:
            file_hash:
                Assigned to the `hash` field of the JWT
            file_ext:
                Assigned to the `file_ext` field of the JWT
            thumbnails_available:
                Assigned to the `thumbnails_available` field of the JWT
            duration:
                Assigned to the `duration` field of the JWT

        Returns:
            Instance of the class with data of the uploaded video.
        """
        jwt_data = FileUploadedResponseJWT(
            exp=FileUploadedResponseJWT.default_expiration_from_now(),
            iss=TokenIssuer.external,
            hash=file_hash,
            file_ext=file_ext,
            thumbnails_available=thumbnails_available,
            duration=duration,
        )
        vid_url, thumb_urls = RequestFileJWTData.get_urls(
            file_hash,
            file_ext,
            temp=True,
            thumb_count=thumbnails_available,
        )
        return cls.parse_obj({
            "jwt": jwt_data.encode(),
            "url": vid_url,
            "thumbnails": thumb_urls,
        })


class FileDoesNotExist(_Error):
    _status_code: int = 404
    error: Literal["file_does_not_exist"] = "file_does_not_exist"
    file_hash: str = Field(exclude=True)

    def _log_response(self, log: Logger) -> None:
        log.error(f"Save request failed; file(s) not found: {self.file_hash}")


class StorageStatus(NodeStatus):
    distributor_nodes: list[str]  # list of base_urls
    num_current_uploads: int


class StorageFileInfo(HashedFileModel):
    size: int  # in MB


class StorageFilesList(BaseResponseModel):
    files: list[StorageFileInfo]


class NotAllFilesDeleted(_Incomplete):
    not_deleted: list[str]


class DistributorStatusDict(BaseResponseModel):
    nodes: dict[str, DistributorStatus]  # keys are base urls


########################
# Request body models: #
# (admin interface)    #

class DistributorNodeInfo(BaseRequestModel):
    base_url: str


class DeleteFilesList(BaseRequestModel):
    hashes: list[str]
