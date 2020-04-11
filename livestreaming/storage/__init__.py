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
    tx_max_rate_mbit: int


storage_logger = logging.getLogger('livestreaming-storage')
storage_settings = StorageSettings()


def start() -> None:
    from livestreaming.network import NetworkInterfaces
    from .api.routes import routes
    storage_settings.load()

    async def on_http_startup(app):
        NetworkInterfaces.get_instance().start_fetching()

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(storage_settings.http_port, routes, on_http_startup, on_http_cleanup)
