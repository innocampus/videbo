from enum import Enum
from typing import Optional

from livestreaming.web import BaseJWTData


class FileType(Enum):
    STORAGE = "storage"
    THUMBNAIL = "thumbnail"
    VIDEO_TEMP = "video_temp"
    THUMBNAIL_TEMP = "thumbnail_temp"


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
