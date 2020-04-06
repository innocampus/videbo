import logging

from livestreaming.web import start_web_server
from livestreaming.settings import SettingsSectionBase


class StorageSettings(SettingsSectionBase):
    _section = "storage"
    http_port: int
    videos_path: str
    public_base_url: str
    max_file_size_mb: int
    thumb_suggestion_count: int
    thumb_height: int
    check_user: str
    binary_file: str
    binary_ffmpeg: str
    binary_ffprobe: str


storage_logger = logging.getLogger('livestreaming-storage')
storage_settings = StorageSettings()


def start() -> None:
    from .api.routes import routes
    storage_settings.load()
    start_web_server(storage_settings.http_port, routes)
