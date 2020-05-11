from enum import Enum
from typing import Optional, List

from videbo.web import BaseJWTData, JSONBaseModel


class FileType(Enum):
    VIDEO = "video"
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
    rid: str  # random string identifying the user


class StorageStatus(JSONBaseModel):
    tx_current_rate: int  # in Mbit/s
    tx_max_rate: int  # in Mbit/s
    rx_current_rate: int  # in Mbit/s
    tx_total: int  # in MB
    rx_total: int  # in MB
    current_connections: Optional[int]  # HTTP connections serving videos
    files_total_size: int  # in MB
    files_count: int
    free_space: int  # in MB
    distributor_nodes: List[str]  # list of base_urls


class DistributorNodeInfo(JSONBaseModel):
    base_url: str
