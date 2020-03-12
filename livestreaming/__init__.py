import configparser
from os.path import abspath, dirname


# Globals that may be useful for all nodes.
class Settings:
    args = {}
    config = configparser.ConfigParser()
    topdir = dirname(dirname(abspath(__file__)))
    internal_api_secret = ''
    lms_api_secret = ''
    dev_mode = False


settings = Settings()
