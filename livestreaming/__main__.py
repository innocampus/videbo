import argparse
import configparser
import logging
import sys

from . import settings


def main() -> None:
    """main entry point for all nodes"""
    # log level
    logging.basicConfig(level=logging.INFO)

    # get CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='path to config file', default=None)
    parser.add_argument('--http-port', help='http api port', default=0, type=int)
    parser.add_argument('mode', help='which server part to start')
    settings.args = parser.parse_args()

    # read config file
    filename = settings.args.config
    if filename is None:
        filename = settings.topdir + "/config.ini"
    settings.config.read(filename)

    # get all config options that are useful for all nodes
    settings.load()

    if settings.general.dev_mode:
        logging.warning('Development mode is enabled. You should enable this mode only during development!')

    # Hand over to node code
    if settings.args.mode == 'broker':
        from livestreaming.broker import start
        start()
    elif settings.args.mode == 'content':
        from livestreaming.content import start
        start()
    elif settings.args.mode == 'encoder':
        from livestreaming.encoder import start
        start()
    elif settings.args.mode == 'manager':
        from livestreaming.manager import start
        start()
    else:
        print("Mode must be manager, encoder, content or broker")
        sys.exit(2)


if __name__ == '__main__':
    main()
