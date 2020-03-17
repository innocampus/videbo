import logging
from pathlib import PurePath, Path

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server


class ContentSettings(SettingsSectionBase):
    _section: str = 'content'
    http_port: int
    hls_temp_dir: PurePath


content_settings = ContentSettings()
logger = logging.getLogger('livestreaming-content')


def start() -> None:
    from .api.routes import routes
    content_settings.load()

    # ensure temp dir exists
    temp_dir = Path(content_settings.hls_temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    start_web_server(content_settings.http_port, routes)
