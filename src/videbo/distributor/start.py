from videbo import settings
from videbo.network import NetworkInterfaces
from videbo.web import start_web_server

from .api.routes import routes
from .files import DistributorFileController


def start(**_kwargs: object) -> None:
    settings.files_path.mkdir(parents=True, exist_ok=True)
    start_web_server(
        routes,
        NetworkInterfaces.app_context,
        DistributorFileController.app_context,
        address=settings.listen_address,
        port=settings.listen_port,
        verbose=settings.dev_mode,
    )
