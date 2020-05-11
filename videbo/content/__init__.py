import logging
import shutil
from pathlib import PurePath, Path

from videbo import settings
from videbo.settings import SettingsSectionBase
from videbo.web import start_web_server


class ContentSettings(SettingsSectionBase):
    _section: str = 'content'
    http_port: int
    hls_temp_dir: PurePath
    max_clients: int


content_settings = ContentSettings()
content_logger = logging.getLogger('videbo-content')


def start() -> None:
    from .api.routes import routes
    from .clients import client_collection
    content_settings.load()

    # ensure temp dir exists and is empty.
    temp_dir = Path(content_settings.hls_temp_dir)
    if temp_dir.is_dir():
        # Remove all files. They may have been left from another run.
        shutil.rmtree(path=temp_dir, onerror=lambda f, p, e: content_logger.error(f"{f} {p}:{e}"))
    temp_dir.mkdir(parents=True, exist_ok=True)

    client_collection.init(content_settings.max_clients)

    start_web_server(content_settings.http_port, routes)
