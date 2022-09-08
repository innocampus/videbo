import logging
import sys
from argparse import ArgumentParser, SUPPRESS
from pathlib import Path
from typing import Any, Union

from .base_settings import DEFAULT_CONFIG_FILE_PATHS, CONFIG_FILE_PATHS_PARAM
from .cli.args import setup_cli_args, run

import videbo
from videbo.storage.start import start as start_storage
from videbo.storage.settings import StorageSettings
from videbo.distributor.start import start as start_distributor
from videbo.distributor.settings import DistributorSettings

# CLI parameters:
APP = 'app'
STORAGE, DISTRIBUTOR, CLI = 'storage', 'distributor', 'cli'
LISTEN_ADDRESS, LISTEN_PORT = 'listen_address', 'listen_port'
_VALID_SETTINGS_KWARGS = {CONFIG_FILE_PATHS_PARAM, LISTEN_ADDRESS, LISTEN_PORT}


def parse_cli() -> dict[str, Any]:
    parser = ArgumentParser(
        prog='videbo',
        description="Launch a video server node or interact with one that is already running."
    )
    parser.add_argument(
        '-c', f'--{CONFIG_FILE_PATHS_PARAM.strip("_").replace("_", "-")}',
        type=path_list,
        dest=CONFIG_FILE_PATHS_PARAM,
        default=SUPPRESS,
        help=f"Comma separated list of paths to config files that will take precedence over all others; "
             f"the following {len(DEFAULT_CONFIG_FILE_PATHS)} paths are always checked first (in that order): "
             f"{','.join(str(p) for p in DEFAULT_CONFIG_FILE_PATHS)}"
    )
    parser.add_argument(
        '-A', f'--{LISTEN_ADDRESS.replace("_", "-")}',
        default=SUPPRESS,
        help="The IP address the node should bind to. Takes precedence over the argument in the config file."
    )
    parser.add_argument(
        '-P', f'--{LISTEN_PORT.replace("_", "-")}',
        type=int,
        default=SUPPRESS,
        help="The port the node should bind to. Takes precedence over the argument in the config file."
    )
    subparsers = parser.add_subparsers(title="Available applications", dest=APP)
    subparsers.add_parser(name=STORAGE, help="Start storage node")
    subparsers.add_parser(name=DISTRIBUTOR, help="Start distributor node")
    setup_cli_args(subparsers.add_parser(name=CLI, help="CLI tool"))
    return vars(parser.parse_args())


def path_list(string: str) -> list[Path]:
    return [Path(path.strip()) for path in string.split(',')]


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    cli_kwargs = parse_cli()
    app = cli_kwargs.pop(APP)
    if app == CLI:
        init_kwargs = {key: cli_kwargs[key] for key in _VALID_SETTINGS_KWARGS if key in cli_kwargs.keys()}
        setattr(videbo, 'storage_settings', StorageSettings(**init_kwargs))
        run(**cli_kwargs)
        return
    settings: Union[StorageSettings, DistributorSettings]
    if app == STORAGE:
        settings, start = StorageSettings(**cli_kwargs), start_storage
        setattr(videbo, 'storage_settings', settings)
    elif app == DISTRIBUTOR:
        settings, start = DistributorSettings(**cli_kwargs), start_distributor
        setattr(videbo, 'distributor_settings', settings)
    else:
        print("Application must be storage or distributor")
        sys.exit(2)
    if settings.dev_mode:
        main_log = logging.getLogger("videbo")
        main_log.setLevel(logging.DEBUG)
        main_log.warning("Development mode is enabled!")
    start()


if __name__ == '__main__':
    main()
