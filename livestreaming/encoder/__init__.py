import logging
from pathlib import PurePath

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server


class EncoderSettings(SettingsSectionBase):
    _section: str = 'encoder'
    http_port: int
    binary_ffmpeg: str
    binary_ffprobe: str
    hls_temp_dir: PurePath


encoder_settings = EncoderSettings()
logger = logging.getLogger('livestreaming-encoder')


def start() -> None:
    from .api.routes import routes
    encoder_settings.load()

    if settings.general.dev_mode:
        routes.static("/data/hls", encoder_settings.hls_temp_dir)
    start_web_server(encoder_settings.http_port, routes)

