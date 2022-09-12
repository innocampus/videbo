from collections.abc import AsyncIterator

from aiohttp.web_app import Application

from videbo import storage_settings as settings
from videbo.network import NetworkInterfaces
from videbo.web import start_web_server

from .api.routes import routes
from .util import FileStorage


async def network_context(_app: Application) -> AsyncIterator[None]:
    NetworkInterfaces.get_instance().start_fetching(settings)
    yield
    NetworkInterfaces.get_instance().stop_fetching()


async def storage_context(_app: Application) -> AsyncIterator[None]:
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


def start() -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(routes, network_context, storage_context, monitoring_context, address=settings.listen_address,
                     port=settings.listen_port, verbose=settings.dev_mode)
