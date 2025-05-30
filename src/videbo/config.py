from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import Any, Optional, TypeVar, Union, TYPE_CHECKING

import tomli
from pydantic import BaseModel as PydanticBaseModel
from pydantic import BaseSettings as PydanticBaseSettings
from pydantic.class_validators import validator
from pydantic.config import Extra
from pydantic.fields import Field, ModelField, SHAPE_LIST, SHAPE_SET
from pydantic.networks import AnyHttpUrl, IPvAnyAddress

from videbo.misc.constants import MEGA, VIDEO_CONTAINER_FORMATS, VIDEO_MIME_TYPES
from videbo.misc.functions import is_subclass

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic.env_settings import SettingsSourceCallable

    from videbo.types import PathT


__all__ = [
    'CONFIG_FILE_PATHS_PARAM',
    'DEFAULT_CONFIG_FILE_NAME',
    'DEFAULT_CONFIG_FILE_PATHS',
    'PROJECT_DIR',
    'DistributionSettings',
    'LMSSettings',
    'MonitoringSettings',
    'Settings',
    'ThumbnailSettings',
    'VideoSettings',
    'WebserverSettings',
    'config_file_settings',
]

M = TypeVar("M", bound=PydanticBaseModel)

log = logging.getLogger(__name__)

_THIS_DIR = Path(__file__).parent
PROJECT_DIR = _THIS_DIR.parent.parent
DEFAULT_CONFIG_FILE_NAME = 'config.toml'
DEFAULT_CONFIG_FILE_PATHS = [
    Path('/etc/videbo', DEFAULT_CONFIG_FILE_NAME),
    Path(PROJECT_DIR, DEFAULT_CONFIG_FILE_NAME),
    Path('.', DEFAULT_CONFIG_FILE_NAME),
]
CONFIG_FILE_PATHS_PARAM = '_config_file_paths'


class SettingsBaseModel(PydanticBaseModel):
    @validator('*', pre=True)
    def split_str(cls, v: str, field: ModelField) -> Union[str, list[str], set[str]]:
        if is_subclass(field.type_, str) and isinstance(v, str):
            if field.shape == SHAPE_LIST:
                return [part.strip() for part in v.split(',')]
            if field.shape == SHAPE_SET:
                return {part.strip() for part in v.split(',')}
        return v

    @validator('*')
    def discard_empty_str_elements(cls, v: str, field: ModelField) -> Union[str, list[str], set[str]]:
        if is_subclass(field.type_, str):
            if isinstance(v, list):
                return [element for element in v if element != '']
            if isinstance(v, set):
                return {element for element in v if element != ''}
        return v

    class Config:
        extra = Extra.forbid
        allow_inf_nan = False
        anystr_strip_whitespace = True


class BaseSettings(PydanticBaseSettings, SettingsBaseModel):
    _config_file_paths: list[Path] = DEFAULT_CONFIG_FILE_PATHS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        config_paths = kwargs.pop(CONFIG_FILE_PATHS_PARAM, [])
        self._config_file_paths = DEFAULT_CONFIG_FILE_PATHS + [
            Path(path) for path in config_paths
        ]
        super().__init__(*args, **kwargs)

    def get_config_file_paths(self) -> list[Path]:
        return self._config_file_paths

    @validator("*", pre=True)
    def none_to_model_defaults(cls, v: Any, field: ModelField) -> Any:
        """Replaces `None` on `SettingsBaseModel` fields with model default"""
        if is_subclass(field.type_, SettingsBaseModel) and v is None:
            v = field.default
        return v

    class Config:
        env_prefix = 'videbo_'
        env_nested_delimiter = '__'
        underscore_attrs_are_private = True
        validate_assignment = True

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable  # noqa: ARG003
        ) -> tuple[Callable[[BaseSettings], dict[str, Any]], ...]:
            return init_settings, env_settings, config_file_settings


def no_slash_at_the_end(string: str) -> str:
    """
    Returns a version of `string` with no forward slash at the end.

    Any number of consecutive slashes at the end of `string` will be cut off.
    Does nothing, if `string` is not a string.
    Intended to be re-used as a Pydantic field validator.
    """
    if isinstance(string, str):
        return re.sub(r"/+$", "", string)
    return string


class WebserverSettings(SettingsBaseModel):
    status_page: Optional[AnyHttpUrl] = None
    x_accel_location: Optional[str] = None
    x_accel_limit_rate_mbit: float = Field(0., ge=0)

    _norm_x_accel_location = validator(
        "x_accel_location",
        allow_reuse=True,
    )(no_slash_at_the_end)

    def get_x_accel_limit_rate(self, *, internal: bool) -> int:
        """
        Returns the "X-Accel-Limit-Rate" header value in bytes.

        Returns zero, if `internal` is `True`.
        """
        if internal:
            return 0
        return int(self.x_accel_limit_rate_mbit * MEGA / 8)


class LMSSettings(SettingsBaseModel):
    api_urls: list[AnyHttpUrl] = Field(default_factory=list)

    _norm_lms_api_urls = validator(
        "api_urls",
        each_item=True,
        allow_reuse=True,
    )(no_slash_at_the_end)


class ThumbnailSettings(SettingsBaseModel):
    suggestion_count: int = Field(3, gt=0)
    height: int = Field(90, gt=0)
    cache_max_mb: float = Field(30.0, ge=0)


class VideoSettings(SettingsBaseModel):
    max_file_size_mb: float = Field(200.0, gt=0)
    binary_file: str = 'file'
    binary_ffmpeg: str = 'ffmpeg'
    binary_ffprobe: str = 'ffprobe'
    check_user: Optional[str] = None
    mime_types_allowed: set[str] = {'video/mp4', 'video/webm'}
    container_formats_allowed: set[str] = {'mp4', 'webm'}
    video_codecs_allowed: set[str] = {'h264', 'vp8'}
    audio_codecs_allowed: set[str] = {'aac', 'vorbis'}


    @validator("mime_types_allowed", each_item=True)
    def ensure_valid_mime_type(cls, v: str) -> str:
        if v not in VIDEO_MIME_TYPES:
            raise ValueError(f"Not a valid video MIME type: {v}")
        return v

    @validator("container_formats_allowed", each_item=True)
    def ensure_valid_format(cls, v: str) -> str:
        if v not in VIDEO_CONTAINER_FORMATS:
            raise ValueError(f"Not a valid video container format: {v}")
        return v

    @property
    def max_file_size_bytes(self) -> int:
        return int(self.max_file_size_mb * MEGA)


class DistributionSettings(SettingsBaseModel):
    static_node_base_urls: list[AnyHttpUrl] = Field(default_factory=list)
    copy_views_threshold: int = Field(3, ge=0)
    views_retention_minutes: float = Field(240., gt=0)
    views_update_freq_minutes: float = Field(30., gt=0)
    node_cleanup_freq_minutes: float = Field(240., gt=0)
    free_space_target_ratio: float = Field(0.1, ge=0, le=1)
    max_parallel_copying_tasks: int = Field(20, ge=1)
    leave_free_space_mb: float = Field(4000.0, ge=0)
    last_request_safety_minutes: float = Field(240., ge=0)
    max_load_file_copy: float = Field(0.99, gt=0, le=1)
    load_threshold_delayed_redirect: float = Field(0.5, ge=0, le=1)

    _norm_node_urls = validator(
        "static_node_base_urls",
        each_item=True,
        allow_reuse=True,
    )(no_slash_at_the_end)


class MonitoringSettings(SettingsBaseModel):
    prom_text_file: Optional[Path] = None
    update_freq_sec: float = Field(15.0, gt=0)


class Settings(BaseSettings):
    listen_address: IPvAnyAddress = '127.0.0.1'  # type: ignore[assignment]
    listen_port: int = Field(9020, ge=0, lt=2**16)
    files_path: Path = Path('/tmp/videbo')  # noqa: S108
    internal_api_secret: str = ''
    external_api_secret: str = ''
    public_base_url: AnyHttpUrl = 'http://localhost:9020'  # type: ignore[assignment]
    forbid_admin_via_proxy: bool = True
    dev_mode: bool = False
    max_temp_storage_hours: float = Field(12., gt=0)
    temp_file_cleanup_freq_hours: float = Field(1., gt=0)
    tx_max_rate_mbit: float = Field(20.0, gt=0)
    max_load_file_serving: float = Field(0.95, gt=0, le=1)
    network_info_fetch_interval: float = Field(10.0, gt=0)
    webserver: WebserverSettings = WebserverSettings()
    lms: LMSSettings = LMSSettings()
    thumbnails: ThumbnailSettings = ThumbnailSettings()
    video: VideoSettings = VideoSettings()
    distribution: DistributionSettings = DistributionSettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    test_video_file_path: Path = Path(PROJECT_DIR, 'tests', 'test_video.mp4')

    # Additional validators:
    _norm_public_base_url = validator(
        "public_base_url",
        allow_reuse=True,
    )(no_slash_at_the_end)

    def make_url(self, path: str = "/", scheme: str = "http") -> str:
        if not path.startswith("/"):
            path = "/" + path
        return f"{scheme}://{self.listen_address}:{self.listen_port}{path}"

    @property
    def temp_file_cleanup_freq(self) -> float:
        """The value of `temp_file_cleanup_freq_hours` in seconds"""
        return self.temp_file_cleanup_freq_hours * 60 * 60

    @property
    def views_retention_seconds(self) -> float:
        """The value of `distribution.views_retention_minutes` in seconds"""
        return self.distribution.views_retention_minutes * 60

    @property
    def views_update_freq(self) -> float:
        """The value of `distribution.views_update_freq_minutes` in seconds"""
        return self.distribution.views_update_freq_minutes * 60

    @property
    def dist_cleanup_freq(self) -> float:
        """The value of `distribution.node_cleanup_freq_minutes` in seconds"""
        return self.distribution.node_cleanup_freq_minutes * 60

    @property
    def dist_last_request_safety_seconds(self) -> float:
        """The value of `distribution.last_request_safety_minutes` in seconds"""
        return self.distribution.last_request_safety_minutes * 60


def config_file_settings(settings: BaseSettings) -> dict[str, Any]:
    """
    Incrementally loads (and updates) settings from all config files.

    Gets the config file paths from `Settings.get_config_file_paths` method.
    Tries available loaders and returns the result in a dictionary.
    Intended to be used in the `BaseSettings.Config.customise_sources` method.
    """
    config = {}
    for path in settings.get_config_file_paths():
        if not path.is_file():
            log.info("No file found at '%s'", str(path.resolve()))
            continue
        log.info("Reading config file '%s'", str(path.resolve()))
        if path.suffix == ".toml":
            config.update(load_toml(path))
        else:
            log.warning("Unknown config file extension '%s'", path.suffix)
    return config


def load_toml(path: PathT) -> dict[str, Any]:
    with Path(path).open("rb") as f:
        return tomli.load(f)
