import logging
from pathlib import PurePath, Path

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server
from .cloud.definitions import CloudInstanceDefsController
from .node_controller import NodeController


class ManagerSettings(SettingsSectionBase):
    _section: str = 'manager'
    http_port: int
    cloud_providers: str
    hetzner_api_token: str
    cloud_deployment: bool
    static_content_nodes_ips: str
    init_static_content_nodes: bool


manager_settings = ManagerSettings()
logger = logging.getLogger('livestreaming-manager')
cloud_definitions = CloudInstanceDefsController()


def start() -> None:
    from .api.routes import routes
    manager_settings.load()

    cloud_definitions.init_from_config(settings)

    nc = NodeController(manager_settings)
    nc.start_loop()

    start_web_server(manager_settings.http_port, routes)

