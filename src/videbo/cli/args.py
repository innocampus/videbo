import sys
from argparse import ArgumentParser
from typing import Any


# CLI commands:
CMD = 'cmd'
SHOW_STATUS = 'status'
FIND_ORPHANS = 'find-orphaned-files'
DELETE = 'delete'
SHOW_DIST_NODES, DISABLE_DIST, ENABLE_DIST = 'show-dist-nodes', 'disable-dist-node', 'enable-dist-node'
URL = 'url'


def setup_cli_args(parser: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(title="Available CLI commands", dest=CMD, required=True)
    subparsers.add_parser(
        name=SHOW_STATUS,
        help="Print status details about main storage node."
    )
    find_orphans = subparsers.add_parser(
        name=FIND_ORPHANS,
        help="Identify files existing in storage that are unknown to any LMS."
    )
    find_orphans.add_argument(
        '-d', f'--{DELETE}',
        action='store_true',
        help="Setting this flag deletes all orphaned files from storage and distributor nodes."
    )
    subparsers.add_parser(
        name=SHOW_DIST_NODES,
        help="Print status details about all distributor nodes."
    )
    disable_dist_node = subparsers.add_parser(
        name=DISABLE_DIST,
        help="Disable a distributor node (do not redirect more requests to this node).")
    disable_dist_node.add_argument(
        URL,
        help="Base URL of the distributor node"
    )
    enable_dist_node = subparsers.add_parser(
        name=ENABLE_DIST,
        help="Enable a previously disabled distributor node.")
    enable_dist_node.add_argument(
        URL,
        help="Base URL of the distributor node"
    )


async def run_cli_command(**kwargs: Any) -> None:
    from videbo.storage.api.client import StorageClient
    from .storage import (
        disable_distributor_node,
        enable_distributor_node,
        find_orphaned_files,
        print_distributor_nodes,
        print_storage_status,
    )
    async with StorageClient() as client:
        if kwargs[CMD] == SHOW_STATUS:
            await print_storage_status(client)
        elif kwargs[CMD] == FIND_ORPHANS:
            await find_orphaned_files(client, delete=kwargs[DELETE])
        elif kwargs[CMD] == SHOW_DIST_NODES:
            await print_distributor_nodes(client)
        elif kwargs[CMD] == DISABLE_DIST:
            await disable_distributor_node(client, kwargs[URL])
        elif kwargs[CMD] == ENABLE_DIST:
            await enable_distributor_node(client, kwargs[URL])
        else:
            print("Invalid cli command argument given.")
            sys.exit(3)
