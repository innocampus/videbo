#!/usr/bin/env python3

"""
Entry point script for starting a node and executing videbo commands.

Run videbo from the CLI with the `-h` flag to receive usage instructions.
Global settings are loaded from all defined sources in this script,
and saved in `videbo.storage_settings` and `videbo.distributor_settings`.
"""

import asyncio
import logging
from argparse import ArgumentParser, SUPPRESS
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Optional

from videbo import settings
from videbo.cli.args import execute_cli_command, setup_cli_args
from videbo.config import DEFAULT_CONFIG_FILE_PATHS, CONFIG_FILE_PATHS_PARAM
from videbo.storage.start import start as start_storage
from videbo.distributor.start import start as start_distributor

# CLI parameters:
MODE = 'mode'
FUNCTION = 'function'
STORAGE, DISTRIBUTOR, CLI = 'storage', 'distributor', 'cli'
LISTEN_ADDRESS, LISTEN_PORT = 'listen_address', 'listen_port'
_VALID_SETTINGS_KWARGS = {CONFIG_FILE_PATHS_PARAM, LISTEN_ADDRESS, LISTEN_PORT}


def path_list(string: str) -> list[Path]:
    return [Path(path.strip()) for path in string.split(',')]


def cli_run(**cli_kwargs: Any) -> None:
    asyncio.run(execute_cli_command(**cli_kwargs))


def parse_cli(args: Optional[Sequence[str]] = None) -> dict[str, Any]:
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
    subparsers = parser.add_subparsers(title="Available modes", dest=MODE)
    parser_storage = subparsers.add_parser(name=STORAGE, help="Start storage node")
    parser_storage.set_defaults(**{FUNCTION: start_storage})
    parser_distributor = subparsers.add_parser(name=DISTRIBUTOR, help="Start distributor node")
    parser_distributor.set_defaults(**{FUNCTION: start_distributor})
    parser_cli = subparsers.add_parser(name=CLI, help="CLI tool")
    parser_cli.set_defaults(**{FUNCTION: cli_run})
    setup_cli_args(parser_cli)
    return vars(parser.parse_args(args))


def prepare_settings(cli_kwargs: dict[str, Any]) -> Path:
    settings_init_kwargs = {}
    for key in _VALID_SETTINGS_KWARGS:
        value = cli_kwargs.pop(key, None)
        if value is not None:
            settings_init_kwargs[key] = value
    mode = cli_kwargs.pop(MODE)
    # TODO: Handle validation errors more gracefully
    settings.__init__(**settings_init_kwargs)  # type: ignore[misc]
    settings_path = Path(".", f".videbo_{mode}_settings.json")
    if not settings.dev_mode:
        return settings_path
    main_log = logging.getLogger("videbo")
    main_log.setLevel(logging.DEBUG)
    try:
        with settings_path.open("w") as f:
            f.write(settings.json(indent=4))
    except PermissionError:
        msg = "Development mode enabled, but no permissions to write %s"
    else:
        msg = "Development mode enabled! Complete settings visible at %s"
    main_log.warning(msg, str(settings_path.resolve()))
    return settings_path


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    cli_kwargs = parse_cli()
    run = cli_kwargs.pop(FUNCTION)
    settings_dump_path = prepare_settings(cli_kwargs)
    try:
        run(**cli_kwargs)
    finally:
        settings_dump_path.unlink(missing_ok=True)


if __name__ == '__main__':
    main()
