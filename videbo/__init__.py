import logging
import sys
from argparse import ArgumentParser
from pathlib import Path

from .settings import Settings


# CLI parameters:
APP = 'app'
STORAGE, DISTRIBUTOR, CLI = 'storage', 'distributor', 'cli'
CONFIG = 'config'
LISTEN_ADDRESS, LISTEN_PORT = 'listen_address', 'listen_port'

# Globals that may be useful for all nodes.
settings = Settings()


def load_general_settings(cli_only_config: bool = False) -> None:
    """Load all settings that are needed for all nodes."""
    logging.basicConfig(level=logging.INFO)
    default_config_file_path = Path(settings.topdir, 'config.ini')
    # Construct CLI parameters:
    parser = ArgumentParser()
    parser.add_argument(
        '-c', f'--{CONFIG}',
        type=Path,
        default=default_config_file_path,
        help=f"Path to config file; defaults to {default_config_file_path}"
    )
    if not cli_only_config:
        parser.add_argument(
            '-P', f'--{LISTEN_PORT}',
            type=int,
            help="http api port"
        )
        parser.add_argument(
            '-H', f'--{LISTEN_ADDRESS}',
            help="http api host"
        )
        subparsers = parser.add_subparsers(title="Available applications", dest=APP)
        subparsers.add_parser(name=STORAGE, help="Start storage node")
        subparsers.add_parser(name=DISTRIBUTOR, help="Start distributor node")
        from .cli.args import setup_cli_args
        setup_cli_args(subparsers.add_parser(name=CLI, help="CLI tool"))
    # Parse CLI arguments:
    settings.args = parser.parse_args()
    # Read config file:
    config_path = getattr(settings.args, CONFIG)
    if not config_path.is_file():
        print(f"Config file does not exist: {config_path}")
        sys.exit(3)
    settings.config.read(config_path)
    # get all config options that are useful for all nodes
    settings.load()
    if settings.general.dev_mode:
        logging.warning("Development mode is enabled. You should enable this mode only during development!")


def call_subprogram():
    # Hand over to node code
    app = getattr(settings.args, APP)
    if app == STORAGE:
        from videbo.storage import start
        start()
    elif app == DISTRIBUTOR:
        from videbo.distributor import start
        start()
    elif app == CLI:
        from .cli.args import run
        run(settings.args)
    else:
        print("Application must be storage or distributor")
        sys.exit(2)
