import asyncio
import os
from aiohttp import ClientTimeout
from asyncio import get_running_loop, Event, wait_for, TimeoutError
from pathlib import Path
from time import time
from typing import Optional, Dict, Union, Set
from videbo.auth import internal_jwt_encode
from videbo.misc import get_free_disk_space, TaskManager
from videbo.web import HTTPClient
from videbo.storage.util import HashedVideoFile
from videbo.storage.api.models import RequestFileJWTData, FileType
from . import logger, distributor_settings


class CopyFileStatus:
    def __init__(self):
        self.event: Event = Event()  # When event is fired, we know that the file was downloaded completely.
        self.loaded_bytes: int = 0
        self.started: float = time()

    async def wait_for(self, max_time: float):
        await wait_for(self.event.wait(), max_time)


class DistributorHashedVideoFile(HashedVideoFile):
    __slots__ = "copy_status", "file_size"

    def __init__(self, file_hash: str, file_extension: str):
        super().__init__(file_hash, file_extension)
        self.copy_status: Optional[CopyFileStatus] = None
        self.file_size: int = -1  # in bytes


class DistributorFileController:
    MAX_WAITING_CLIENTS = 60

    def __init__(self):
        # file hash -> Event if the node is still downloading the file right now (event is fired when load completed)
        self.files: Dict[str, DistributorHashedVideoFile] = {}
        self.files_total_size: int = 0  # in bytes
        self.files_being_copied: Set[DistributorHashedVideoFile] = set()
        self.base_path: Optional[Path] = None
        self.waiting: int = 0  # number of clients waiting for a file being downloaded

    def load_file_list(self, base_path: Path):
        """Initialize object and load all existing file names."""
        self.base_path = base_path
        for obj in base_path.glob('**/*'):
            if obj.is_file():
                file_split = obj.name.split('.')
                file_hash = file_split[0]
                if file_split[-1] == 'tmp':
                    # Do not consider .tmp files. Delete them.
                    obj.unlink()
                    continue

                file = DistributorHashedVideoFile(file_hash, "." + file_split[1])
                file.file_size = obj.stat().st_size
                self.files[file_hash] = file
                self.files_total_size += file.file_size
                if len(self.files) < 20:
                    logger.info(f"Found video {obj}")

        if len(self.files) >= 20:
            logger.info("Skip logging the other files that were found")
        logger.info(f"Found {len(self.files)} videos")

    def get_path(self, file: HashedVideoFile, temp: bool = False) -> Path:
        if temp:
            return Path(self.base_path, file.hash[0:2], file.hash + file.file_extension + ".tmp")
        else:
            return Path(self.base_path, file.hash[0:2], file.hash + file.file_extension)

    def get_path_or_nginx_redirect(self, file: HashedVideoFile) -> Union[Path, str]:
        """Returns a str with the information where to internally redirect the request when nginx X-Accel-Redirect
        is enabled. Otherwise a Path to the filesystem."""
        loc = distributor_settings.nginx_x_accel_location
        if loc:
            return f"{loc}/{file.hash[0:2]}/{file.hash}{file.file_extension}"

        return self.get_path(file)

    async def file_exists(self, file: HashedVideoFile, wait: int) -> bool:
        """A file with the hash exists. If the file is being downloaded right now, wait for it.
        :raises TimeoutError
        """
        found = self.files.get(file.hash)
        if found:
            if found.copy_status:
                if self.waiting >= self.MAX_WAITING_CLIENTS:
                    raise TooManyWaitingClients()

                try:
                    self.waiting += 1
                    await found.copy_status.wait_for(wait)
                finally:
                    self.waiting -= 1
            return True

        return False

    async def get_free_space(self) -> int:
        """Returns free space in MB excluding the space that should be empty."""
        free = await get_free_disk_space(str(self.base_path))
        return max(free - distributor_settings.leave_free_space_mb, 0)

    def copy_file(self, file: HashedVideoFile, from_url: str, expected_file_size: int) \
            -> DistributorHashedVideoFile:
        if file.hash in self.files:
            # File is already there or it is downloaded right now.
            logger.info(f"File {file} is already there or it is downloaded right now.")
            return self.files[file.hash]

        logger.info(f"Start copying file {file} from {from_url}")
        copy_status = CopyFileStatus()
        new_file = DistributorHashedVideoFile(file.hash, file.file_extension)
        new_file.copy_status = copy_status
        self.files[file.hash] = new_file
        self.files_being_copied.add(new_file)

        async def do_copy():
            # load file
            temp_path = self.get_path(file, True)
            file_obj = None
            try:
                # ensure dir exists
                await get_running_loop().run_in_executor(None, os.makedirs, temp_path.parent, 0o755, True)

                # open file
                file_obj = await get_running_loop().run_in_executor(None, temp_path.open, 'wb', 0)
                free_space = await self.get_free_space()

                # prepare request
                jwt_data = RequestFileJWTData.construct(type=FileType.VIDEO.value, hash=file.hash,
                                                        file_ext=file.file_extension, rid="", role="node")
                jwt = internal_jwt_encode(jwt_data)
                headers = { "Authorization": "Bearer " + jwt }
                timeout = ClientTimeout(total=120*60)

                async with HTTPClient.session.request("GET", from_url + "/file", headers=headers,
                                                      timeout=timeout) as response:
                    if response.status != 200:
                        logger.error(f"Error when copying file {file} from {from_url}: got http status {response.status}")
                        raise CopyFileError()

                    # Check if we have enough space for this file.
                    new_file.file_size = expected_file_size
                    file_size_mb = new_file.file_size / 1024 / 1024
                    if file_size_mb > free_space:
                        logger.error(f"Error when copying file {file} from {from_url}: Not enough space, "
                                     f"free space {free_space:.1f} MB, file is {file_size_mb:.1f} MB")
                        raise CopyFileError()

                    # Load file
                    last_update_time = time()
                    while True:
                        data = await response.content.read(1024 * 1024)
                        if len(data) == 0:
                            break
                        copy_status.loaded_bytes += len(data)
                        await get_running_loop().run_in_executor(None, file_obj.write, data)

                        # If the download is taking much time, periodically print status.
                        if (time() - last_update_time) > 120:
                            last_update_time = time()
                            loaded_mb = copy_status.loaded_bytes / 1024 / 1024
                            percent = 100 * (copy_status.loaded_bytes / expected_file_size)
                            logger.info(f"Still copying, copied {loaded_mb:.1f}/{file_size_mb:.1f} MB "
                                        f"({percent:.1f} %) until now of file {file}")

                    if copy_status.loaded_bytes != expected_file_size:
                        logger.error(f"Error when copying file {file} from {from_url}: Loaded "
                                     f"{copy_status.loaded_bytes} bytes,"
                                     f"but expected {expected_file_size} bytes.")
                        raise CopyFileError()
                    logger.info(f"Copied file {file} ({file_size_mb:.1f} MB) from {from_url}")
            except:
                # Set event to wake up all waiting tasks even though we don't have the file.
                copy_status.event.set()
                self.files.pop(file.hash)
                logger.exception(f"Error when copying file {file} from {from_url}")
                raise CopyFileError()
            finally:
                self.files_being_copied.discard(new_file)
                self.files_total_size += expected_file_size
                if file_obj:
                    await get_running_loop().run_in_executor(None, file_obj.close)

            # move to final location (without .tmp suffix)
            final_path = self.get_path(file)
            try:
                await get_running_loop().run_in_executor(None, temp_path.rename, final_path)
            except OSError:
                logger.exception(f"Error when renaming file {temp_path} to {final_path}")
                self.files.pop(file.hash)
            copy_status.event.set()
            new_file.copy_status = None

        task = asyncio.create_task(do_copy())
        TaskManager.fire_and_forget_task(task)
        return new_file

    async def delete_file(self, file: HashedVideoFile) -> None:
        try:
            dist_file = self.files.pop(file.hash)
        except KeyError:
            return

        self.files_total_size -= dist_file.file_size
        path = self.get_path(dist_file)
        await get_running_loop().run_in_executor(None, path.unlink)


file_controller = DistributorFileController()


class CopyFileError(Exception):
    pass


class TooManyWaitingClients(Exception):
    pass
