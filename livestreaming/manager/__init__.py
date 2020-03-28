import logging
from pathlib import PurePath, Path

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server
from .node_controller import NodeController


class ManagerSettings(SettingsSectionBase):
    _section: str = 'manager'
    http_port: int
    cloud_providers: str
    hetzner_api_token: str
    cloud_deployment: bool


manager_settings = ManagerSettings()
logger = logging.getLogger('livestreaming-manager')


def start() -> None:
    from .api.routes import routes
    manager_settings.load()

    if manager_settings.cloud_deployment:
        nc = NodeController(manager_settings)
        nc.start_loop()

    start_web_server(manager_settings.http_port, routes)

