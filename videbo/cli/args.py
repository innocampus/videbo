import sys
from asyncio import get_event_loop
from argparse import ArgumentParser, Namespace

from videbo.web import HTTPClient
from .storage import (get_status, find_orphaned_files, get_distributor_nodes, enable_distributor_node,
                      disable_distributor_node)


# CLI commands:
SHOW_STATUS = 'status'
FIND_ORPHANS = 'find-orphaned-files'
SHOW_DIST_NODES, DISABLE_DIST, ENABLE_DIST = 'show-dist-nodes', 'disable-dist-node', 'enable-dist-node'


def setup_cli_args(parser: ArgumentParser) -> None:
    subparsers = parser.add_subparsers(title="Available CLI commands", dest="cmd", required=True)
    subparsers.add_parser(
        name=SHOW_STATUS,
        help="Print status details about main storage node."
    )
    find_orphans = subparsers.add_parser(
        name=FIND_ORPHANS,
        help="Identify files existing in storage that are unknown to any LMS."
    )
    find_orphans.add_argument(
        '-d', '--delete',
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
        'url',
        help="Base URL of the distributor node"
    )
    enable_dist_node = subparsers.add_parser(
        name=ENABLE_DIST,
        help="Enable a previously disabled distributor node.")
    enable_dist_node.add_argument(
        'url',
        help="Base URL of the distributor node"
    )


def run(args: Namespace) -> None:
    HTTPClient.create_client_session()
    try:
        if args.cmd == SHOW_STATUS:
            fut = get_status()
        elif args.cmd == FIND_ORPHANS:
            fut = find_orphaned_files(args)
        elif args.cmd == SHOW_DIST_NODES:
            fut = get_distributor_nodes()
        elif args.cmd == DISABLE_DIST:
            fut = disable_distributor_node(args.url)
        elif args.cmd == ENABLE_DIST:
            fut = enable_distributor_node(args.url)
        else:
            print("Invalid cli command argument given.")
            sys.exit(3)
        get_event_loop().run_until_complete(fut)
    finally:
        get_event_loop().run_until_complete(HTTPClient.close_all(None))
