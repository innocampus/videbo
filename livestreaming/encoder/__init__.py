import logging
from pathlib import PurePath, Path

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
    passwd_length: int
    rtmp_internal_ports: str
    rtmp_public_ports: str
    rtmps_cert: str
    max_streams: int


encoder_settings = EncoderSettings()
logger = logging.getLogger('livestreaming-encoder')


def start() -> None:
    from .api.routes import routes
    encoder_settings.load()

    # ensure temp dir exists
    temp_dir = Path(encoder_settings.hls_temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    if settings.general.dev_mode:
        routes.static("/data/hls", encoder_settings.hls_temp_dir)
    start_web_server(encoder_settings.http_port, routes)
