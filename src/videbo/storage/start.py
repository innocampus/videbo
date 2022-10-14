from collections.abc import AsyncIterator

from aiohttp.web_app import Application

from videbo import settings
from videbo.lms_api import LMS
from videbo.network import NetworkInterfaces
from videbo.web import start_web_server

from .api.routes import routes
from .util import FileStorage


async def network_context(_app: Application) -> AsyncIterator[None]:
    NetworkInterfaces.get_instance().start_fetching()
    yield
    NetworkInterfaces.get_instance().stop_fetching()


async def lms_api_context(_app: Application) -> AsyncIterator[None]:
    LMS.add(*settings.lms_api_urls)
    yield


async def storage_context(_app: Application) -> AsyncIterator[None]:
    FileStorage.get_instance()  # init instance
    yield  # No cleanup necessary


async def monitoring_context(_app: Application) -> AsyncIterator[None]:
    if settings.monitoring.prom_text_file:
        from .monitoring import Monitoring
        await Monitoring.get_instance().run()
        yield
        await Monitoring.get_instance().stop()
    else:
        yield


def start(**_kwargs: object) -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(
        routes,
        network_context,
        lms_api_context,
        storage_context,
        monitoring_context,
        address=settings.listen_address,
        port=settings.listen_port,
        verbose=settings.dev_mode,
    )
