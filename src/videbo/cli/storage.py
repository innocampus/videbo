from distutils.util import strtobool

from videbo.storage.api.client import StorageClient as Client
from videbo.storage.api.models import StorageFileInfo


def print_response(http_code: int) -> None:
    if http_code == 200:
        print("Successful! Please check storage log output.")
    else:
        print(f"HTTP response code {http_code}! Please check storage log output.")


async def print_storage_status(client: Client) -> None:
    status, data = await client.get_status()
    if status == 200:
        print(data.json(indent=4))
    else:
        print_response(status)


async def find_orphaned_files(client: Client, *, delete: bool) -> None:
    """
    Awaits results from the filtered files request and depending on arguments passed,
    either deletes or simply lists all orphaned files currently in storage,
    by calling the delete_files coroutine or list_files function respectively.
    """
    confirm = input(
        "You are about to start searching for/identifying orphaned files. "
        "This may request each LMS for knowledge of all the stored files. "
        "Depending on the number of files stored and the number of LMS registered, "
        "this may take a long time.\nProceed? (yes/no) "
    )
    if not strtobool(confirm):
        print("Aborted.")
        return
    print("Querying storage for orphaned files...")
    files = await client.get_filtered_files(orphaned=True)
    if files is None:
        print("Error requesting filtered files list from storage node!")
        return
    num_files = len(files)
    if num_files == 0:
        print("No orphaned files found.")
        return
    total_size = round(sum(file.file_size for file in files) / 1024 / 1024, 1)
    print(f"Found {num_files} orphaned files with a total size of {total_size} MB.")
    if not delete:
        list_files(*files)
        print("\nIf you want to delete all orphaned files, use the command with the --delete flag.")
        return

    confirm = input("Are you sure, you want to delete them from storage? (yes/no) ")
    if not strtobool(confirm):
        print("Aborted.")
        return
    status, data = await client.delete_files(*files)
    if status != 200:
        print("Request failed. Please check the storage logs.")
        return
    if data['status'] == 'ok':
        print("All files have been successfully deleted from storage.")
        return
    print("Error! The following files could not be deleted from storage:")
    for file_hash in data['not_deleted']:
        print(file_hash)
    print("Please check the storage logs for more information.")


def list_files(*files: StorageFileInfo) -> None:
    """
    Prints a pretty table of stored files (name and extension) and their size (in MB).
    """
    h_name, h_size = "File name (hash & extension)", "Size"
    # SHA256 hash in hexadecimal is 64 characters plus the dot and three character file extension:
    name_length = 64 + 4
    # digits left of the decimal plus the decimal point plus the precision plus 3 characters of " MB":
    left_of_decimal, precision = 4, 1
    size_length = left_of_decimal + 1 + min(1, precision) + 3

    horizontal_sep = f"+ {'-' * name_length} + {'-' * size_length} +"
    print()
    print(horizontal_sep)
    print(f"| {h_name:{name_length}} | {h_size:>{size_length}} |")
    print(horizontal_sep)
    for file in files:
        size_str = f"{round(file.file_size / 1024 / 1024, 1)} MB"
        print(f"| {str(file):{name_length}} | {size_str:>{size_length}} |")
    print(horizontal_sep)


async def print_distributor_nodes(client: Client) -> None:
    status, data = await client.get_distributor_nodes()
    if status == 200:
        print(data.json(indent=4))
    else:
        print_response(status)


async def disable_distributor_node(client: Client, base_url: str) -> None:
    print_response(
        await client.set_distributor_state(base_url, enabled=False)
    )


async def enable_distributor_node(client: Client, base_url: str) -> None:
    print_response(
        await client.set_distributor_state(base_url, enabled=True)
    )
