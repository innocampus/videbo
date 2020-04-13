import argparse
import logging
import sys
from pathlib import Path
from .settings import Settings


# Globals that may be useful for all nodes.
settings = Settings()


def load_general_settings(cli_only_config: bool = False) -> None:
    """Load all settings that are needed for all nodes."""
    # log level
    logging.basicConfig(level=logging.INFO)

    # get CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='path to config file', default=None)
    if not cli_only_config:
        parser.add_argument('--http-port', help='http api port', default=0, type=int)
        parser.add_argument('mode', help='which server part to start')
    settings.args = parser.parse_args()

    # read config file
    if settings.args.config is not None:
        config_file = Path(settings.args.config)
    else:
        config_file = Path(settings.topdir + "/config.ini")

    if not config_file.is_file():
        print(f"Config file does not exist: {config_file}")
        sys.exit(3)

    settings.config.read(config_file)

    # get all config options that are useful for all nodes
    settings.load()

    if settings.general.dev_mode:
        logging.warning('Development mode is enabled. You should enable this mode only during development!')#


def call_node():
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
    elif settings.args.mode == 'storage':
        from livestreaming.storage import start
        start()
    elif settings.args.mode == 'distributor':
        from livestreaming.distributor import start
        start()
    else:
        print("Mode must be manager, encoder, content, broker, storage or distributor")
        sys.exit(2)
