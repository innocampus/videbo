import logging
from pathlib import PurePath, Path

from videbo import settings
from videbo.settings import SettingsSectionBase
from videbo.web import start_web_server


class ManagerSettings(SettingsSectionBase):
    _section: str = 'manager'
    http_port: int
    cloud_providers: str
    dns_provider: str
    cloud_deployment: bool
    static_storage_node_base_url: str
    static_distributor_node_base_urls: str
    init_static_content_nodes: bool
    remove_orphaned_nodes: bool
    dynamic_node_name_prefix: str  # always ends with - (a hyphen)
    db_file: PurePath
    influx_url: str
    influx_database: str
    influx_username: str
    influx_password: str

    def load(self):
        super().load()
        if self.dynamic_node_name_prefix[-1] != '-':
            self.dynamic_node_name_prefix = self.dynamic_node_name_prefix + '-'


manager_settings = ManagerSettings()
logger = logging.getLogger('videbo-manager')


def start() -> None:
    from .api.routes import routes
    from .cloud.definitions import CloudInstanceDefsController
    from .node_controller import NodeController
    from .storage_controller import storage_controller
    from .db import Database
    from .monitoring import Monitoring

    manager_settings.load()

    # load cloud provider and instance definitions/settings from config
    cloud_definitions = CloudInstanceDefsController()
    cloud_definitions.init_from_config(settings)

    db = Database()
    db.connect(str(manager_settings.db_file))

    nc = NodeController.init_instance(manager_settings, cloud_definitions, db)

    mon = Monitoring(nc)

    async def on_http_startup(app):
        await nc.start()
        storage_controller.init(nc)
        await mon.run()

    async def on_http_cleanup(app):
        await db.disconnect()

    start_web_server(manager_settings.http_port, routes, on_http_startup, on_http_cleanup)

