from argparse import ArgumentParser


def setup_cli_args(parser: ArgumentParser):
    subparsers = parser.add_subparsers(title="Available CLI commands", dest="cmd")
    subparsers.add_parser("create-node", help="Create a new dynamic node")
    subparsers.add_parser("remove-node", help="Remove/shutdown a dynamic node")
