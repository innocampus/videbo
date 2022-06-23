import logging
from typing import AsyncIterator

from aiohttp.web_app import Application


logger = logging.getLogger('videbo-distributor')


def start() -> None:
    from videbo import distributor_settings as settings
    from videbo.web import start_web_server
    from videbo.network import NetworkInterfaces
    from .files import file_controller
    from .api.routes import routes

    if settings.dev_mode:
        logging.warning("Development mode is enabled. You should enable this mode only during development!")

    # Ensure dir exists and load all files in it if there are any.
    settings.files_path.mkdir(parents=True, exist_ok=True)
    file_controller.load_file_list(settings.files_path)

    async def network_context(_app: Application) -> AsyncIterator[None]:
        NetworkInterfaces.get_instance().start_fetching(settings.server_status_page, logger)
        yield
        await NetworkInterfaces.get_instance().stop_fetching()

    start_web_server(routes, network_context, address=settings.listen_address, port=settings.listen_port,
                     verbose=settings.dev_mode)
