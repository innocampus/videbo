from argparse import ArgumentParser, SUPPRESS
from typing import Any

from videbo.storage.api.client import StorageClient
from .storage import (
    disable_distributor_node,
    enable_distributor_node,
    find_orphaned_files,
    print_distributor_nodes,
    print_storage_status,
)


# CLI parameters:
CMD = 'cli_cmd'
YES = 'yes_all'
SHOW_STATUS = 'status'
FIND_ORPHANS = 'find-orphaned-files'
DELETE = 'delete'
SHOW_DIST_NODES = 'show-dist-nodes'
DISABLE_DIST, ENABLE_DIST = 'disable-dist-node', 'enable-dist-node'
URL = 'url'


def setup_cli_args(parser: ArgumentParser) -> None:
    parser.add_argument(
        '-y', f'--{YES.replace("_", "-")}',
        action='store_true',
        default=SUPPRESS,
        help="If this flag is set, every confirmation prompt (yes/no) "
             "will automatically be passed with `yes`.",
    )
    subparsers = parser.add_subparsers(
        title="Available CLI commands",
        required=True,
    )

    parser_status = subparsers.add_parser(
        name=SHOW_STATUS,
        help="Print status details about main storage node.",
    )
    parser_status.set_defaults(**{CMD: print_storage_status})

    parser_orphans = subparsers.add_parser(
        name=FIND_ORPHANS,
        help="Identify files in storage that are unknown to all LMS.",
    )
    parser_orphans.set_defaults(**{CMD: find_orphaned_files})
    parser_orphans.add_argument(
        '-d', f'--{DELETE}',
        action='store_true',
        help="Setting this flag deletes all orphaned files from all nodes.",
    )

    parser_dist_status = subparsers.add_parser(
        name=SHOW_DIST_NODES,
        help="Print status details about all distributor nodes.",
    )
    parser_dist_status.set_defaults(**{CMD: print_distributor_nodes})

    parser_dist_disable = subparsers.add_parser(
        name=DISABLE_DIST,
        help="Disable a distributor node. "
             "Prevents redirecting more requests to the node.",
    )
    parser_dist_disable.set_defaults(**{CMD: disable_distributor_node})
    parser_dist_disable.add_argument(
        URL,
        help="Base URL of the distributor node",
    )

    parser_dist_enable = subparsers.add_parser(
        name=ENABLE_DIST,
        help="Enable a previously disabled distributor node.",
    )
    parser_dist_enable.set_defaults(**{CMD: enable_distributor_node})
    parser_dist_enable.add_argument(
        URL,
        help="Base URL of the distributor node",
    )


async def execute_cli_command(**kwargs: Any) -> None:
    run = kwargs.pop(CMD)
    async with StorageClient() as client:
        await run(client, **kwargs)
