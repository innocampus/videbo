import os
from distutils.util import strtobool

from videbo.misc import MEGA
from videbo.storage.api.client import StorageClient as Client
from videbo.storage.api.models import StorageFileInfo


os.system("")  # Enables some ANSI codes on Windows machines

GREEN = "\033[32m"
YELLOW = "\033[33m"
BOLD = "\033[1m"
RESET = "\033[0m"


def print_response(http_code: int) -> None:
    """
    Prints a generic response to stdout based on the given HTTP status code.

    Any code other than 200 will result in a warning-style message.
    """
    if http_code == 200:
        print(f"{GREEN}Request was successful!{RESET}")
    else:
        print(
            f"{YELLOW}HTTP response code {http_code}!{RESET} "
            f"Please check storage log for details."
        )


async def show_storage_status(client: Client) -> None:
    """Requests the current status of the storage node and prints it."""
    status, data = await client.get_status()
    if status == 200:
        print(data.json(indent=4))
    else:
        print_response(status)


async def find_orphaned_files(
    client: Client,
    *,
    delete: bool,
    yes_all: bool = False,
) -> None:
    """
    Requests the storage to identify all orphaned files.

    Args:
        client:
            The `StorageClient` instance to use for making the requests.
        delete:
            If `True`, upon identifying all orphaned files, a request for
            batch deletion is made for those files.
        yes_all (optional):
            If `False` (default), an additional confirmation prompt is
            presented before proceeding with the identification request,
            and one more confirmation prompt is issued before deletion.
            If `True`, confirmation is assumed and all prompts are skipped.
    """
    if not yes_all and not strtobool(input(
        "You are about to start searching for orphaned files. This may "
        "request each LMS for knowledge of all the stored files. Depending "
        "on the number of files stored and the number of LMS registered, "
        "this may take a long time.\nProceed? (yes/no) "
    )):
        print("Aborted.")
        return
    print("Querying storage for orphaned files...")
    status, data = await client.get_filtered_files(orphaned=True)
    if status != 200:
        print_response(status)
        return
    num_files = len(data.files)
    if num_files == 0:
        print("No orphaned files found.")
        return
    total_size = round(sum(file.size for file in data.files) / MEGA, 1)
    print(
        f"Found {num_files} orphaned files "
        f"with a total size of {total_size} MB."
    )
    if not delete:
        list_files(*data.files)
        print(
            f"\nIf you want to delete all orphaned files, "
            f"use the command with the {BOLD}--delete{RESET} flag."
        )
        return

    if not yes_all and not strtobool(
        input("Are you sure, you want to delete them? (yes/no) ")
    ):
        print("Aborted.")
        return
    status, response_dict = await client.delete_files(*data.files)
    if status != 200:
        print_response(status)
        return
    if response_dict["status"] == "ok":
        print(
            f"{GREEN}Request was successful!{RESET}\n"
            f"All orphaned files have been deleted from storage."
        )
        return
    print(f"{YELLOW}Warning!{RESET} The following files were not deleted:")
    for file_hash in response_dict["not_deleted"]:
        print(file_hash)
    print("Please check the storage logs for more information.")


def list_files(*files: StorageFileInfo) -> None:
    """
    Prints a pretty table of the provided files to stdout.

    Listing includes name, file extension, and size (in MB).
    """
    h_name, h_size = "File name (hash & extension)", "Size"
    # SHA256 hash in hexadecimal is 64 characters plus the dot
    # plus three characters for file extension:
    name_length = 64 + 4
    # digits left of the decimal plus the decimal point
    # plus the precision plus 3 characters of " MB":
    left_of_decimal, precision = 4, 1
    size_length = left_of_decimal + 1 + min(1, precision) + 3

    horizontal_sep = f"+ {'-' * name_length} + {'-' * size_length} +"
    print()
    print(horizontal_sep)
    print(f"| {h_name:{name_length}} | {h_size:>{size_length}} |")
    print(horizontal_sep)
    for file in files:
        size_str = f"{round(file.size / MEGA, 1)} MB"
        print(f"| {str(file):{name_length}} | {size_str:>{size_length}} |")
    print(horizontal_sep)


async def show_distributor_nodes(client: Client) -> None:
    """Requests the current status of distributor nodes and prints it."""
    status, data = await client.get_distributor_nodes()
    if status == 200:
        print(data.json(indent=4))
    else:
        print_response(status)


async def disable_distributor_node(client: Client, url: str) -> None:
    """Requests disabling the distributor node with the given base URL"""
    print_response(
        await client.set_distributor_state(url, enabled=False)
    )


async def enable_distributor_node(client: Client, url: str) -> None:
    """Requests enabling the distributor node with the given base URL"""
    print_response(
        await client.set_distributor_state(url, enabled=True)
    )
