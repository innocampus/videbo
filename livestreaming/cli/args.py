import sys
from asyncio import get_event_loop
from argparse import ArgumentParser
from typing import Any
from livestreaming.web import HTTPClient


def setup_cli_args(parser: ArgumentParser):
    subparsers = parser.add_subparsers(title="Available CLI commands", dest="cmd", required=True)

    subparsers.add_parser("manager-show-nodes", help="List of all nodes")

    create_dist_node = subparsers.add_parser("manager-create-distributor-node",
                                             help="Create a new dynamic distributor node")
    create_dist_node.add_argument("definition", help="name of the instance definition (section name in config)")
    create_dist_node.add_argument("bound_to_storage_url", help="Base URL of the storage")

    disable_dist_node = subparsers.add_parser("manager-disable-distributor-node",
                                              help="Disable a distributor node (do not redirect more requests to "
                                                   "this node)")
    disable_dist_node.add_argument("node", help="name of the node instance")

    enable_dist_node = subparsers.add_parser("manager-enable-distributor-node",
                                             help="Enable a distributor node again that was disabled before")
    enable_dist_node.add_argument("node", help="name of the node instance")

    remove_dist_node = subparsers.add_parser("manager-remove-node", help="Remove/shutdown a dynamic node")
    remove_dist_node.add_argument("node", help="name of the dynamic node instance")


def run(args: Any):
    from .manager import get_all_nodes, create_distributor_node, set_distributor_status, remove_distributor_node

    HTTPClient.create_client_session()
    try:
        if args.cmd == "manager-show-nodes":
            fut = get_all_nodes(args)
        elif args.cmd == "manager-create-distributor-node":
            fut = create_distributor_node(args)
        elif args.cmd == "manager-disable-distributor-node":
            fut = set_distributor_status(args, False)
        elif args.cmd == "manager-enable-distributor-node":
            fut = set_distributor_status(args, True)
        elif args.cmd == "manager-remove-node":
            fut = remove_distributor_node(args)
        else:
            print("Invalid cli command argument given.")
            sys.exit(3)

        get_event_loop().run_until_complete(fut)

    finally:
        get_event_loop().run_until_complete(HTTPClient.close_all(None))
