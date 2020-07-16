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
        parser.add_argument('--http-host', help='http api host', default='', type=str)
        subparsers = parser.add_subparsers(title="Available applications", dest="app")
        subparsers.add_parser("manager", help="Start manager node")
        subparsers.add_parser("storage", help="Start storage node")
        subparsers.add_parser("distributor", help="Start distributor node")
        cli_tool = subparsers.add_parser("cli", help="CLI tool")
        from .cli.args import setup_cli_args
        setup_cli_args(cli_tool)

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


def call_subprogram():
    # Hand over to node code
    if settings.args.app == 'manager':
        from videbo.manager import start
        start()
    elif settings.args.app == 'storage':
        from videbo.storage import start
        start()
    elif settings.args.app == 'distributor':
        from videbo.distributor import start
        start()
    elif settings.args.app == 'cli':
        from .cli.args import run
        run(settings.args)
    else:
        print("Application must be manager, storage or distributor")
        sys.exit(2)
