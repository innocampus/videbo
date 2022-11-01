from videbo import settings
from videbo.network import NetworkInterfaces
from videbo.web import start_web_server

from .api.routes import routes
from .files import DistributorFileController


def start() -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(
        routes,
        settings.listen_address,
        settings.listen_port,
        cleanup_contexts=(
            NetworkInterfaces.app_context,
            DistributorFileController.app_context,
        ),
        verbose=settings.dev_mode,
    )
