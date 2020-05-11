import logging

from pathlib import PurePath
from videbo.web import start_web_server, ensure_url_does_not_end_with_slash
from videbo.settings import SettingsSectionBase


class StorageSettings(SettingsSectionBase):
    _section = "storage"
    http_port: int
    videos_path: PurePath
    public_base_url: str
    max_file_size_mb: int
    thumb_suggestion_count: int
    thumb_height: int
    check_user: str
    binary_file: str
    binary_ffmpeg: str
    binary_ffprobe: str
    tx_max_rate_mbit: int
    server_status_page: str
    copy_to_dist_views_threshold: int
    reset_views_every_hours: int

    def load(self):
        super().load()
        self.public_base_url = ensure_url_does_not_end_with_slash(self.public_base_url)

        # at least 1 hour
        if self.reset_views_every_hours < 1:
            self.reset_views_every_hours = 1


storage_logger = logging.getLogger('videbo-storage')
storage_settings = StorageSettings()


def start() -> None:
    from videbo.network import NetworkInterfaces
    from .api.routes import routes
    storage_settings.load()

    async def on_http_startup(app):
        from .util import FileStorage
        NetworkInterfaces.get_instance().start_fetching(storage_settings.server_status_page, storage_logger)
        storage = FileStorage.get_instance()  # init instance
        storage.load_file_list()  # This actually does blocking io, but it is only done once.

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(storage_settings.http_port, routes, on_http_startup, on_http_cleanup)
