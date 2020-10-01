import configparser
import argparse
import sys
from os.path import abspath, dirname
from pathlib import PurePath
from typing import get_type_hints


class SettingsSectionBase:
    """Use this as a base for a class that represents the settings in one section of the config file."""
    _section: str = 'OVERWRITE'

    def load(self):
        from . import settings
        type_hints = get_type_hints(self.__class__)
        for name, setting_type in type_hints.items():
            if name.startswith('_'):
                continue
            if name == 'http_port' and getattr(settings.args, 'http_port', None):
                value = settings.args.http_port
            elif setting_type is float:
                value = float(settings.get_config(self._section, name))
            elif setting_type is int:
                value = int(settings.get_config(self._section, name))
            elif setting_type is bool:
                value = settings.get_config_bool(self._section, name)
            elif setting_type is str:
                value = settings.get_config(self._section, name)
            elif setting_type is PurePath:
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
    certificate_crt_path: PurePath
    certificate_key_path: PurePath


class LMSSettings(SettingsSectionBase):
    _section = 'lms'
    api_secret: str
    moodle_base_urls: str


class Settings:
    """Central settings storage."""
    args: argparse.Namespace
    config: configparser.ConfigParser
    topdir: str
    general: GeneralSettings
    lms: LMSSettings

    def __init__(self):
        self.args = argparse.Namespace()
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

