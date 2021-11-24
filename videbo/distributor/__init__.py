import logging
from typing import AsyncIterator
from pathlib import Path
from aiohttp.web_app import Application
from videbo.web import start_web_server, ensure_url_does_not_end_with_slash
from videbo.settings import SettingsSectionBase


class DistributorSettings(SettingsSectionBase):
    _section = "distributor"
    listen_address: str
    listen_port: int
    bound_to_storage_base_url: str
    tx_max_rate_mbit: int
    files_path: Path
    leave_free_space_mb: int
    nginx_x_accel_location: str
    nginx_x_accel_limit_rate_mbit: float
    server_status_page: str
    last_request_safety_hours: int

    def load(self):
        super().load()
        self.bound_to_storage_base_url = ensure_url_does_not_end_with_slash(self.bound_to_storage_base_url)
        self.nginx_x_accel_location = ensure_url_does_not_end_with_slash(self.nginx_x_accel_location)


logger = logging.getLogger('videbo-distributor')
distributor_settings = DistributorSettings()


def start() -> None:
    from videbo.network import NetworkInterfaces
    from .files import file_controller
    from .api.routes import routes
    distributor_settings.load()

    # Ensure dir exists and load all files in it if there are any.
    distributor_settings.files_path.mkdir(parents=True, exist_ok=True)
    file_controller.load_file_list(distributor_settings.files_path)

    async def network_context(_app: Application) -> AsyncIterator[None]:
        NetworkInterfaces.get_instance().start_fetching(distributor_settings.server_status_page, logger)
        yield
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(distributor_settings.listen_port, routes, network_context)
