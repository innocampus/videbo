from collections.abc import AsyncIterator

from aiohttp.web_app import Application

from videbo import settings
from videbo.network import network_context
from videbo.web import start_web_server

from .api.routes import routes
from .files import DistributorFileController


async def distributor_context(_app: Application) -> AsyncIterator[None]:
    DistributorFileController.get_instance()  # init instance
    yield  # No cleanup necessary


def start(**_kwargs: object) -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(
        routes,
        network_context,
        distributor_context,
        address=settings.listen_address,
        port=settings.listen_port,
        verbose=settings.dev_mode,
    )
