import logging

from pathlib import PurePath
from videbo.web import start_web_server, ensure_url_does_not_end_with_slash
from videbo.settings import SettingsSectionBase


class StorageSettings(SettingsSectionBase):
    _section = 'storage'
    http_port: int
    files_path: PurePath
    public_base_url: str
    max_file_size_mb: int
    thumb_suggestion_count: int
    thumb_height: int
    check_user: str
    binary_file: str
    binary_ffmpeg: str
    binary_ffprobe: str
    tx_max_rate_mbit: int
    static_dist_node_base_urls: str
    copy_to_dist_views_threshold: int
    reset_views_every_hours: int
    dist_free_space_target_ratio: float
    max_parallel_copying_tasks: int
    dist_redirect_prevent_hours: float
    nginx_x_accel_location: str
    nginx_x_accel_limit_rate_mbit: float
    thumb_cache_max_mb: int
    server_status_page: str
    prom_text_file: PurePath
    prom_update_freq_sec: float

    def load(self):
        super().load()
        self.public_base_url = ensure_url_does_not_end_with_slash(self.public_base_url)
        self.nginx_x_accel_location = ensure_url_does_not_end_with_slash(self.nginx_x_accel_location)

        # at least 1 hour
        if self.reset_views_every_hours < 1:
            self.reset_views_every_hours = 1


storage_logger = logging.getLogger('videbo-storage')
storage_settings = StorageSettings()


def start() -> None:
    from videbo import settings
    from videbo.network import NetworkInterfaces
    from .api.routes import routes, access_logger
    storage_settings.load()

    if not settings.general.dev_mode:
        # Do not log simple video accesses when not in dev mode.
        access_logger.setLevel(logging.WARNING)

    async def on_http_startup(app):
        from .util import FileStorage
        from .monitoring import Monitoring
        NetworkInterfaces.get_instance().start_fetching(storage_settings.server_status_page, storage_logger)
        FileStorage.get_instance()  # init instance
        await Monitoring.get_instance().run()

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(storage_settings.http_port, routes, on_http_startup, on_http_cleanup)
