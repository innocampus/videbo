from collections.abc import AsyncIterator

from aiohttp.web_app import Application

from videbo import settings
from videbo.lms_api import LMS
from videbo.network import NetworkInterfaces
from videbo.web import start_web_server

from .api.routes import routes
from .file_controller import StorageFileController


async def monitoring_context(_app: Application) -> AsyncIterator[None]:
    if settings.monitoring.prom_text_file:
        from .monitoring import Monitoring  # noqa: PLC0415
        await Monitoring.get_instance().run()
        yield
        await Monitoring.get_instance().stop()
    else:
        yield


def start() -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(
        routes,
        str(settings.listen_address),
        settings.listen_port,
        cleanup_contexts=(
            NetworkInterfaces.app_context,
            LMS.app_context,
            StorageFileController.app_context,
            monitoring_context,
        ),
        verbose=settings.dev_mode,
    )
