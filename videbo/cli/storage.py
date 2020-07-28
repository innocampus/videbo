from typing import Optional, List
from argparse import Namespace
from urllib.parse import urlencode
from distutils.util import strtobool

from videbo.storage.api.models import StorageFilesList, StorageFileInfo, DeleteFilesList
from videbo.web import HTTPClient


def get_storage_url(path: str) -> str:
    from videbo import settings
    port = settings.get_config('storage', 'http_port')
    return f'http://localhost:{port}{path}'


async def find_orphaned_files(args: Namespace) -> None:
    """
    Awaits results from the filtered files request and depending on arguments passed,
    either deletes or simply lists all orphaned files currently in storage,
    by calling the delete_files coroutine or list_files function respectively.
    """
    print("Querying storage for orphaned files...")
    files = await get_filtered_files(orphaned=True)
    if files is None:
        print("Error requesting filtered files list from storage node!")
        return
    num_files = len(files)
    if num_files == 0:
        print("No orphaned files found.")
        return
    total_size = round(sum(file.file_size for file in files) / 1024 / 1024, 1)
    print(f"Found {num_files} orphaned files with a total size of {total_size} MB.")
    if args.delete:
        confirm = input("Are you sure, you want to delete them from storage? (yes/no) ")
        if not strtobool(confirm):
            print("Aborted.")
            return
        await delete_files(*files)
    else:
        list_files(*files)
        print("\nIf you want to delete all orphaned files, use the command with the --delete flag.")


async def get_filtered_files(**kwargs) -> Optional[List[StorageFileInfo]]:
    """
    Makes a GET request to the storage node's files endpoint to receive a list of stored files.
    Any keyword arguments passed are encoded into the url query string, and may be used to filter the results.
    If a 200 response is received, a list of files (matching filter parameters) is returned.
    Any other response causes None to be returned.
    """
    url = get_storage_url(f'/api/storage/files?{urlencode(kwargs)}')
    ret: StorageFilesList
    status, ret = await HTTPClient.internal_request_admin('GET', url, None, StorageFilesList)
    if status == 200:
        return ret.files


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
        print(f"| {file} | {size_str:>{size_length}} |")
    print(horizontal_sep)


async def delete_files(*files: StorageFileInfo) -> None:
    """
    Makes a POST request to perform a batch deletion of files in storage.
    Prints out hashes of any files that could not be deleted.
    """
    url = get_storage_url('/api/storage/delete')
    data = DeleteFilesList(hashes=[f.hash for f in files])
    status_code, ret = await HTTPClient.internal_request_admin('POST', url, data)
    if status_code != 200:
        print(f"Request failed. Please check the storage logs.")
        return
    if ret['status'] == 'ok':
        print("All files have been successfully deleted from storage.")
        return
    print("Error! The following files could not be deleted from storage:")
    for file_hash in ret['not_deleted']:
        print(file_hash)
    print(f"Please check the storage logs for more information.")
