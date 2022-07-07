import logging
import sys
from configparser import ConfigParser
from pathlib import Path
from distutils.util import strtobool
from typing import Any, Callable, ClassVar, Dict, List, Optional, Set, Tuple, Union

from pydantic import BaseSettings, validator
from pydantic.env_settings import SettingsSourceCallable
from pydantic.fields import ModelField, SHAPE_LIST, SHAPE_SET

from videbo.misc import ensure_url_does_not_end_with_slash as normalize_url


log = logging.getLogger(__name__)

_THIS_DIR = Path(__file__).parent
PROJECT_DIR = _THIS_DIR.parent.parent
DEFAULT_CONFIG_FILE_NAME = 'config.ini'
DEFAULT_CONFIG_FILE_PATHS = [
    Path('/etc/videbo', DEFAULT_CONFIG_FILE_NAME),
    Path(PROJECT_DIR, DEFAULT_CONFIG_FILE_NAME),
    Path('.', DEFAULT_CONFIG_FILE_NAME),
]
CONFIG_FILE_PATHS_PARAM = '_config_file_paths'


class AbstractBaseSettings(BaseSettings):
    _section: ClassVar[str] = ''
    _config_file_paths: ClassVar[List[Path]] = DEFAULT_CONFIG_FILE_PATHS

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._config_file_paths.extend(kwargs.pop(CONFIG_FILE_PATHS_PARAM, []))
        super().__init__(*args, **kwargs)

    def get_section(self) -> str:
        return self._section

    def get_config_file_paths(self) -> List[Path]:
        return self._config_file_paths

    class Config:
        env_prefix = 'videbo_'
        underscore_attrs_are_private = True

        @classmethod
        def customise_sources(
                cls,
                init_settings: SettingsSourceCallable,
                env_settings: SettingsSourceCallable,
                file_secret_settings: SettingsSourceCallable
        ) -> Tuple[Callable[['CommonSettings'], Dict[str, Any]], ...]:
            return init_settings, env_settings, ini_config_settings_source


class CommonSettings(AbstractBaseSettings):
    _section = 'DEFAULT'
    listen_address: str = '127.0.0.1'
    listen_port: int = 9010
    files_path: Path = Path('/tmp/videbo')
    internal_api_secret: str = ''
    external_api_secret: str = ''
    lms_base_urls: List[str] = []
    dev_mode: bool = False
    tx_max_rate_mbit: float = 20.0
    server_status_page: Optional[str] = None
    nginx_x_accel_location: Optional[str] = None
    nginx_x_accel_limit_rate_mbit: float = 0.0

    @validator('*', pre=True)
    def split_str(cls, v: str, field: ModelField) -> Union[str, List[str], Set[str]]:
        if field.type_ is str and isinstance(v, str):
            if field.shape == SHAPE_LIST:
                return [part.strip() for part in v.split(',')]
            if field.shape == SHAPE_SET:
                return {part.strip() for part in v.split(',')}
        return v

    @validator('*')
    def discard_empty_str_elements(cls, v: str, field: ModelField) -> Union[str, List[str], Set[str]]:
        if field.type_ is str:
            if isinstance(v, list):
                return [element for element in v if element != '']
            if isinstance(v, set):
                return {element for element in v if element != ''}
        return v

    @validator('lms_base_urls', each_item=True)
    def normalize_lms_base_urls(cls, url: str) -> str:
        return normalize_url(url)

    _norm_nginx_x_accel_location = validator('nginx_x_accel_location', allow_reuse=True)(normalize_url)


def ini_config_settings_source(settings: CommonSettings) -> Dict[str, Any]:
    """
    Incrementally loads (and updates) settings from all config files that can be found as returned by the
    `Settings.get_config_file_paths` method and returns the result in a dictionary.
    This function is intended to be used as a settings source in the `Config.customise_sources` method.
    """
    config = {}
    for path in settings.get_config_file_paths():
        if not path.is_file():
            log.info(f"No file found at '{path}'")
            continue
        log.info(f"Reading config file '{path}'")
        config.update(load_ini_config(path, settings))
    return config


def load_ini_config(path: Path, settings: CommonSettings) -> Dict[str, Any]:
    """
    Loads settings from a single section in an `.ini`-style configuration file.
    NOTE: As per construction of the `ConfigParser` class, parameters from the DEFAULT section will also be included,
    unless overridden in the specific section.
    Expects the Settings instance to already point to a valid configuration file
    and its `get_section` method to return a section name in the configuration file.
    """
    if not path.is_file():
        print(f"Config file does not exist: {path}")
        sys.exit(3)
    parser = ConfigParser()
    parser.read(path)
    config: Dict[str, Any] = dict(parser[settings.get_section()])
    # Perform type conversion for each value.
    for name, value in config.items():
        assert isinstance(value, str)
        field = settings.__fields__.get(name)
        if field is None:
            continue  # Unknown field names should be caught by the model class itself
        if field.type_ is bool:
            config[name] = strtobool(value)
        elif field.type_ is Path and value == '':
            config[name] = None
        else:
            config[name] = field.type_(value)
    return config
