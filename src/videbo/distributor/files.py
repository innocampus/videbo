from __future__ import annotations
import os
from asyncio import get_running_loop, wait_for
from asyncio.exceptions import TimeoutError as AsyncTimeoutError
from asyncio.locks import Event
from collections.abc import AsyncIterator
from logging import getLogger
from pathlib import Path
from time import time
from typing import Optional

from aiohttp.web_app import Application

from videbo import settings
from videbo.client import Client
from videbo.misc import MEGA
from videbo.misc.functions import get_free_disk_space, rel_path
from videbo.misc.task_manager import TaskManager
from videbo.models import Role, TokenIssuer
from videbo.hashed_file import HashedFile
from videbo.storage.api.models import RequestFileJWTData, FileType
from videbo.distributor.api.models import DistributorCopyFileStatus


log = getLogger(__name__)


class CopyFileStatus:
    def __init__(self) -> None:
        self.event: Event = Event()  # When event is fired, we know that the file was downloaded completely.
        self.loaded_bytes: int = 0
        self.started: float = time()

    async def wait_for(self, max_time: float) -> None:
        await wait_for(self.event.wait(), max_time)


class DistributorHashedVideoFile(HashedFile):
    __slots__ = 'copy_status', 'file_size', 'last_requested'

    def __init__(self, file_hash: str, file_ext: str) -> None:
        super().__init__(file_hash, file_ext)
        self.copy_status: Optional[CopyFileStatus] = None
        self.file_size: int = -1  # in bytes
        self.last_requested: int = -1  # UNIX timestamp (seconds); -1 means never/unknown


class DistributorFileController:
    _instance: Optional[DistributorFileController] = None

    MAX_WAITING_CLIENTS = 60

    def __init__(self, path: Path) -> None:
        # file hash -> Event if the node is still downloading the file right now (event is fired when load completed)
        self.files: dict[str, DistributorHashedVideoFile] = {}
        self.files_total_size: int = 0  # in bytes
        self.files_being_copied: set[DistributorHashedVideoFile] = set()
        self.base_path: Path = path
        self.waiting: int = 0  # number of clients waiting for a file being downloaded
        self.http_client: Client = Client()

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        cls.get_instance()  # init instance
        yield  # No cleanup necessary

    @classmethod
    def get_instance(cls) -> DistributorFileController:
        if cls._instance is None:
            cls._instance = DistributorFileController(settings.files_path)
            cls._instance._load_file_list()
        return cls._instance

    def _load_file_list(self) -> None:
        """Initialize object and load all existing file names."""
        for obj in self.base_path.glob('**/*'):
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
                    log.info(f"Found video {obj}")

        if len(self.files) >= 20:
            log.info("Skip logging the other files that were found")
        log.info(f"Found {len(self.files)} videos")

    def get_path(self, file: HashedFile, temp: bool = False, relative: bool = False) -> Path:
        """
        Returns the path to a video file that supposedly exists on the node.

        Args:
            file: Self-explanatory
            temp: If `True`, the string `.tmp` is appended to the filename.
            relative: If `True` the node's base path is omitted from the start of the path.
        """
        name = str(file)
        if temp:
            name += '.tmp'
        return rel_path(name) if relative else Path(self.base_path, rel_path(name))

    async def file_exists(self, file_hash: str, wait: int) -> bool:
        try:
            await self.get_file(file_hash=file_hash, wait=wait)
        except (NoSuchFile, TooManyWaitingClients, AsyncTimeoutError):
            return False
        else:
            return True

    async def get_file(self, file_hash: str, wait: int = 60) -> DistributorHashedVideoFile:
        """
        If a file with the provided hash is controlled by the distributor, return the representing object.
        If the file is being downloaded right now, wait for it.

        Args:
            file_hash: Self-explanatory
            wait (optional): Maximum number of seconds to wait, if the file is being copied.

        Raises:
            NoSuchFile:
                if no file with a hash like this is controlled by this node
            TooManyWaitingClients:
                self-explanatory
            asyncio.TimeoutError:
                if during the specified maximum `wait` time, the file was not fully copied
        """
        try:
            dist_file = self.files[file_hash]
        except KeyError:
            raise NoSuchFile()
        if dist_file.copy_status:
            if self.waiting >= self.MAX_WAITING_CLIENTS:
                raise TooManyWaitingClients()
            try:
                self.waiting += 1
                await dist_file.copy_status.wait_for(wait)  # May raise asyncio.TimeoutError
            finally:
                self.waiting -= 1
        return dist_file

    async def get_free_space(self) -> float:
        """Returns free space in MB excluding the space that should be empty."""
        free = await get_free_disk_space(str(self.base_path))
        return max(free - settings.distribution.leave_free_space_mb, 0.)

    def copy_file(self, file: HashedFile, from_url: str, expected_file_size: int) \
            -> DistributorHashedVideoFile:
        if file.hash in self.files:
            # File is already there or it is downloaded right now.
            log.info(f"File {file} is already there or it is downloaded right now.")
            return self.files[file.hash]

        log.info(f"Start copying file {file} from {from_url}")
        copy_status = CopyFileStatus()
        new_file = DistributorHashedVideoFile(file.hash, file.ext)
        new_file.copy_status = copy_status
        self.files[file.hash] = new_file
        self.files_being_copied.add(new_file)

        async def do_copy() -> None:
            # load file
            temp_path = self.get_path(file, True)
            file_obj = None
            try:
                # ensure dir exists
                await get_running_loop().run_in_executor(None, os.makedirs, temp_path.parent, 0o755, True)

                # open file
                file_obj = await get_running_loop().run_in_executor(None, temp_path.open, 'wb', 0)
                free_space = await self.get_free_space()

                # Check if we have enough space for this file.
                new_file.file_size = expected_file_size
                file_size_mb = new_file.file_size / MEGA
                if file_size_mb > free_space:
                    log.error(f"Error when copying file {file} from {from_url}: Not enough space, "
                              f"free space {free_space:.1f} MB, file is {file_size_mb:.1f} MB")
                    raise CopyFileError()

                jwt = RequestFileJWTData(
                    exp=int(time()) + 300,  # expires in 5 minutes
                    iss=TokenIssuer.internal,
                    role=Role.node,
                    type=FileType.VIDEO,
                    hash=file.hash,
                    file_ext=file.ext,
                    rid='',
                )
                last_update_time = time()
                async for data in self.http_client.request_file_read(
                    from_url + "/file",
                    jwt,
                    chunk_size=MEGA,
                    timeout=2. * 60 * 60,
                ):
                    copy_status.loaded_bytes += len(data)
                    await get_running_loop().run_in_executor(None, file_obj.write, data)
                    # If the download is taking a lot of time, periodically print status.
                    if (time() - last_update_time) > 120:
                        last_update_time = time()
                        loaded_mb = copy_status.loaded_bytes / MEGA
                        percent = 100 * (copy_status.loaded_bytes / expected_file_size)
                        log.info(f"Still copying, copied {loaded_mb:.1f}/{file_size_mb:.1f} MB "
                                 f"({percent:.1f} %) until now of file {file}")
                if copy_status.loaded_bytes != expected_file_size:
                    log.error(f"Error when copying file {file} from {from_url}: Loaded "
                              f"{copy_status.loaded_bytes} bytes, but expected {expected_file_size} bytes.")
                    raise CopyFileError()
                log.info(f"Copied file {file} ({file_size_mb:.1f} MB) from {from_url}")
            except Exception:
                # Set event to wake up all waiting tasks even though we don't have the file.
                copy_status.event.set()
                self.files.pop(file.hash)
                log.exception(f"Error when copying file {file} from {from_url}")
                raise CopyFileError()
            finally:
                self.files_being_copied.discard(new_file)
                if file_obj:
                    await get_running_loop().run_in_executor(None, file_obj.close)

            # move to final location (without .tmp suffix)
            final_path = self.get_path(file)
            try:
                await get_running_loop().run_in_executor(None, temp_path.rename, final_path)
            except OSError:
                log.exception(f"Error when renaming file {temp_path} to {final_path}")
                self.files.pop(file.hash)
            self.files_total_size += expected_file_size
            copy_status.event.set()
            new_file.copy_status = None

        TaskManager.fire_and_forget(do_copy(), name="copy_file_from_node")
        return new_file

    async def delete_file(self, file_hash: str, safe: bool = True) -> None:
        """
        Deletes file from disk.

        Args:
            file_hash: Self-explanatory
            safe (optional): If `True`, safety period from config is checked before deletion.

        Raises:
            NoSuchFile:
                if no file with a hash like this is controlled by this node
            NotSafeToDelete:
                if `safe` is True and the file in question was last requested within the configured safety interval

        Returns:
            `None` upon successful deletion
        """
        dist_file = self.files.get(file_hash)
        if dist_file is None:
            raise NoSuchFile()
        cutoff_time = time() - (settings.distribution.last_request_safety_minutes * 60)
        if safe and dist_file.last_requested > cutoff_time:
            raise NotSafeToDelete()
        del self.files[file_hash]
        self.files_total_size -= dist_file.file_size
        path = self.get_path(dist_file)
        await get_running_loop().run_in_executor(None, path.unlink)

    def get_copy_file_status(self) -> list[DistributorCopyFileStatus]:
        """Returns a list of `DistributorCopyFileStatus` objects, one for each file currently being copied"""
        now = time()
        return [
            DistributorCopyFileStatus(
                hash=file.hash, file_ext=file.ext, loaded=file.copy_status.loaded_bytes,
                file_size=file.file_size, duration=now - file.copy_status.started
            ) for file in self.files_being_copied if file.copy_status
        ]


class CopyFileError(Exception):
    pass


class TooManyWaitingClients(Exception):
    pass


class NoSuchFile(Exception):
    pass


class NotSafeToDelete(Exception):
    pass
