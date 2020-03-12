import argparse
import configparser
import logging
import sys
from . import settings


def main() -> None:
    # log level
    logging.basicConfig(level=logging.INFO)

    # get CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='path to config file', default=None)
    parser.add_argument('mode', help='which server part to start')
    settings.args = parser.parse_args()

    # read config file
    filename = settings.args.config
    if filename is None:
        filename = settings.topdir + "/config.ini"
    settings.config.read(filename)

    # get all config options that are useful for all nodes
    try:
        settings.internal_api_secret = settings.config.get('general', 'api_secret')
        settings.lms_api_secret = settings.config.get('lms', 'api_secret')
        settings.dev_mode = settings.config.get('general', 'dev_mode')

    except configparser.NoOptionError as e:
        print("Missing option in file " + filename + ": " + str(e))
        sys.exit(1)

    if settings.dev_mode:
        logging.warning('Development mode is enabled. You should enable this mode only during development!')

    # Hand over to node code
    if settings.args.mode == 'manager':
        from livestreaming.manager import start
        start()
    else:
        print("Mode must manager, encoder, content or broker")
        sys.exit(2)


if __name__ == '__main__':
    main()
