import logging
from pathlib import PurePath, Path

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server


class ManagerSettings(SettingsSectionBase):
    _section: str = 'manager'
    http_port: int
    cloud_providers: str
    dns_provider: str
    cloud_deployment: bool
    static_content_node_base_urls: str
    init_static_content_nodes: bool
    remove_orphaned_nodes: bool
    dynamic_node_name_prefix: str  # always ends with - (a hyphen)

    def load(self):
        super().load()
        if self.dynamic_node_name_prefix[-1] != '-':
            self.dynamic_node_name_prefix = self.dynamic_node_name_prefix + '-'


manager_settings = ManagerSettings()
logger = logging.getLogger('livestreaming-manager')


def start() -> None:
    from .api.routes import routes
    from .cloud.definitions import CloudInstanceDefsController
    from .node_controller import NodeController

    manager_settings.load()

    # load cloud provider and instance definitions/settings from config
    cloud_definitions = CloudInstanceDefsController()
    cloud_definitions.init_from_config(settings)

    nc = NodeController(manager_settings, cloud_definitions)

    async def on_http_startup(app):
        from .node_types import ContentNode

        if manager_settings.cloud_deployment:
            await nc.start()
            node = await nc.start_content_node(1)
            app['contentnode'] = node

    async def on_http_cleanup(app):
        pass

    start_web_server(manager_settings.http_port, routes, on_http_startup, on_http_cleanup)

