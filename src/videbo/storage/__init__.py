import logging
from collections.abc import AsyncIterator

from aiohttp.web_app import Application


storage_logger = logging.getLogger('videbo-storage')


def start() -> None:
    from videbo.web import start_web_server
    from videbo.network import NetworkInterfaces
    from .api.routes import routes, access_logger
    from videbo import storage_settings as settings

    if settings.dev_mode:
        logging.warning("Development mode is enabled. You should enable this mode only during development!")
    else:
        access_logger.setLevel(logging.ERROR)

    async def network_context(_app: Application) -> AsyncIterator[None]:
        NetworkInterfaces.get_instance().start_fetching(settings.server_status_page, storage_logger)
        yield
        await NetworkInterfaces.get_instance().stop_fetching()

    async def storage_context(_app: Application) -> AsyncIterator[None]:
        from .util import FileStorage
        FileStorage.get_instance()  # init instance
        yield  # No cleanup necessary

    async def monitoring_context(_app: Application) -> AsyncIterator[None]:
        if settings.prom_text_file:
            from .monitoring import Monitoring
            await Monitoring.get_instance().run()
            yield
            await Monitoring.get_instance().stop()
        else:
            yield

    start_web_server(routes, network_context, storage_context, monitoring_context, address=settings.listen_address,
                     port=settings.listen_port, access_logger=access_logger)
