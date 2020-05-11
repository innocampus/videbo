import logging
from pathlib import PurePath, Path
from videbo.web import start_web_server, ensure_url_does_not_end_with_slash
from videbo.settings import SettingsSectionBase


class DistributorSettings(SettingsSectionBase):
    _section = "distributor"
    http_port: int
    bound_to_storage_base_url: str
    tx_max_rate_mbit: int
    files_path: PurePath
    leave_free_space_mb: int
    nginx_x_accel_location: str
    nginx_x_accel_limit_rate_kbit: int
    server_status_page: str

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
    path = Path(distributor_settings.files_path)
    path.mkdir(parents=True, exist_ok=True)
    file_controller.load_file_list(path)

    async def on_http_startup(app):
        NetworkInterfaces.get_instance().start_fetching(distributor_settings.server_status_page, logger)

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(distributor_settings.http_port, routes, on_http_startup, on_http_cleanup)
