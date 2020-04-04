import configparser
import sys
from os.path import abspath, dirname
from pathlib import PurePath
from typing import get_type_hints


class SettingsSectionBase:
    """Use this as a base for a class that represents the settings in one section of the config file."""
    _section: str = 'OVERWRITE'

    def load(self):
        from . import settings
        types = get_type_hints(self)
        for name, type in types.items():
            if name[0] == '_':
                continue

            if name == 'http_port' and settings.args.http_port:
                value = settings.args.http_port
            elif type is int:
                value = int(settings.get_config(self._section, name))
            elif type is bool:
                value = settings.get_config_bool(self._section, name)
            elif type is str:
                value = settings.get_config(self._section, name)
            elif type is PurePath:
                value = PurePath(settings.get_config(self._section, name))
            else:
                raise UnknownSettingsDataType()

            setattr(self, name, value)


class UnknownSettingsDataType(Exception):
    pass


class GeneralSettings(SettingsSectionBase):
    _section = 'general'
    internal_api_secret: str
    dev_mode: bool
    domain: str
    wildcard_certificate_path: PurePath


class LMSSettings(SettingsSectionBase):
    _section = 'lms'
    api_secret: str


class Settings:
    """Central settings storage."""
    args: dict
    config: configparser.ConfigParser
    topdir: str
    general: GeneralSettings
    lms: LMSSettings

    def __init__(self):
        self.args = {}
        self.config = configparser.ConfigParser()
        self.topdir = dirname(dirname(abspath(__file__)))
        self.general = GeneralSettings()
        self.lms = LMSSettings()

    def load(self):
        self.general.load()
        self.lms.load()

    def get_config(self, section: str, option: str) -> str:
        try:
            return self.config.get(section, option)
        except (configparser.NoOptionError, configparser.NoSectionError) as e:
            print("Missing option in config file " + str(e))
            sys.exit(3)

    def get_config_bool(self, section: str, option: str) -> bool:
        try:
            return self.config.getboolean(section, option)
        except (configparser.NoOptionError, configparser.NoSectionError) as e:
            print("Missing option in config file " + str(e))
            sys.exit(3)

