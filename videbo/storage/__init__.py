import logging
from typing import AsyncIterator
from pathlib import Path
from aiohttp.web_app import Application
from videbo.web import start_web_server, ensure_url_does_not_end_with_slash
from videbo.settings import SettingsSectionBase


class StorageSettings(SettingsSectionBase):
    _section = 'storage'
    listen_address: str
    listen_port: int
    files_path: Path
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
    nginx_x_accel_location: str
    nginx_x_accel_limit_rate_mbit: float
    thumb_cache_max_mb: int
    server_status_page: str
    prom_text_file: Path
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
    from .monitoring import Monitoring
    from .api.routes import routes, access_logger
    storage_settings.load()

    if not settings.general.dev_mode:
        # Do not log simple video accesses when not in dev mode.
        access_logger.setLevel(logging.WARNING)

    async def network_context(_app: Application) -> AsyncIterator[None]:
        NetworkInterfaces.get_instance().start_fetching(storage_settings.server_status_page, storage_logger)
        yield
        await NetworkInterfaces.get_instance().stop_fetching()

    async def storage_context(_app: Application) -> AsyncIterator[None]:
        from .util import FileStorage
        FileStorage.get_instance()  # init instance
        yield  # No cleanup necessary

    async def monitoring_context(_app: Application) -> AsyncIterator[None]:
        await Monitoring.get_instance().run()
        yield
        await Monitoring.get_instance().stop()

    start_web_server(storage_settings.listen_port, routes, network_context, storage_context, monitoring_context)
