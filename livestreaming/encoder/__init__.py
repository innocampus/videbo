import logging
import shutil
from pathlib import PurePath, Path
from typing import Optional

from livestreaming import settings
from livestreaming.settings import SettingsSectionBase
from livestreaming.web import start_web_server


class EncoderSettings(SettingsSectionBase):
    _section: str = 'encoder'
    http_port: int
    binary_ffmpeg: str
    binary_ffprobe: str
    binary_socat: str
    hls_temp_dir: PurePath
    rtmp_internal_ports: str
    rtmp_public_ports: str
    max_streams: int


encoder_settings = EncoderSettings()
logger = logging.getLogger('livestreaming-encoder')


def start() -> None:
    from .api.routes import routes
    encoder_settings.load()

    # ensure temp dir exists and is empty.
    temp_dir = Path(encoder_settings.hls_temp_dir)
    if temp_dir.is_dir():
        # Remove all files. They may have been left from another run.
        shutil.rmtree(path=temp_dir, onerror=lambda f, p, e: logger.error(f"{f} {p}:{e}"))
    temp_dir.mkdir(parents=True, exist_ok=True)

    if settings.general.dev_mode:
        routes.static("/data/hls", encoder_settings.hls_temp_dir)
    start_web_server(encoder_settings.http_port, routes)
