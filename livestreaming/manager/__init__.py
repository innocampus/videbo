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
    static_encoder_node_base_urls: str
    static_broker_node_base_url: str
    static_storage_node_base_url: str
    static_distributor_node_base_urls: str
    init_static_content_nodes: bool
    remove_orphaned_nodes: bool
    dynamic_node_name_prefix: str  # always ends with - (a hyphen)
    streams_content_node_distribution_algorithm: str
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
logger = logging.getLogger('livestreaming-manager')


def start() -> None:
    from .api.routes import routes
    from .cloud.definitions import CloudInstanceDefsController
    from .node_controller import NodeController
    from .streams import stream_collection
    from .storage_controller import StorageDistributorController
    from .db import Database

    manager_settings.load()

    # load cloud provider and instance definitions/settings from config
    cloud_definitions = CloudInstanceDefsController()
    cloud_definitions.init_from_config(settings)

    db = Database()
    db.connect(str(manager_settings.db_file))

    nc = NodeController(manager_settings, cloud_definitions, db)
    storage_controller = StorageDistributorController()

    async def on_http_startup(app):
        from .node_types import ContentNode

        stream_collection.init(nc)
        await nc.start()
        storage_controller.init(nc)

        if manager_settings.cloud_deployment:
            #node = await nc.start_content_node(1)
            #node = await nc.start_distributor_node(0, 'https://test1.com')
            #node = await nc.start_distributor_node(0, 'https://test2.com')
            #app['contentnode'] = node
            pass

    async def on_http_cleanup(app):
        await db.disconnect()

    start_web_server(manager_settings.http_port, routes, on_http_startup, on_http_cleanup)

