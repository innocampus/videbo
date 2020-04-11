import logging
from pathlib import PurePath, Path
from livestreaming.web import start_web_server
from livestreaming.settings import SettingsSectionBase


class DistributorSettings(SettingsSectionBase):
    _section = "storage"
    http_port: int
    bound_to_storage_base_url: str
    tx_max_rate_mbit: int
    files_path: PurePath
    leave_free_space_mb: int
    nginx_x_accel_location: str
    nginx_x_accel_limit_rate_kb: int


logger = logging.getLogger('livestreaming-distributor')
distributor_settings = DistributorSettings()


def start() -> None:
    from livestreaming.network import NetworkInterfaces
    from .files import file_controller
    from .api.routes import routes
    distributor_settings.load()

    # Ensure dir exists and load all files in it if there are any.
    Path(distributor_settings.files_path).mkdir(parents=True, exist_ok=True)
    file_controller.load_file_list(distributor_settings.files_path)

    async def on_http_startup(app):
        NetworkInterfaces.get_instance().start_fetching()

    async def on_http_cleanup(app):
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(distributor_settings.http_port, routes, on_http_startup, on_http_cleanup)
