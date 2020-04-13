import logging

from livestreaming.web import start_web_server, ensure_url_does_not_end_with_slash
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

    def load(self):
        super().load()
        self.public_base_url = ensure_url_does_not_end_with_slash(self.public_base_url)


storage_logger = logging.getLogger('livestreaming-storage')
storage_settings = StorageSettings()


def start() -> None:
    from livestreaming.network import NetworkInterfaces
    from .api.routes import routes
    storage_settings.load()

    async def on_http_startup(app):
        from .util import FileStorage
        NetworkInterfaces.get_instance().start_fetching()
        FileStorage.get_instance()  # init instance

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(storage_settings.http_port, routes, on_http_startup, on_http_cleanup)
