from __future__ import annotations
from asyncio.exceptions import TimeoutError as AsyncTimeoutError
from collections.abc import AsyncIterator, Iterator
from logging import getLogger
from io import FileIO
from pathlib import Path
from time import time
from typing import ClassVar, Optional, cast

from aiohttp.web_app import Application

from videbo import settings
from videbo.client import Client
from videbo.misc.constants import MAX_NUM_FILES_TO_PRINT, MEGA
from videbo.misc.functions import get_free_disk_space, rel_path, run_in_default_executor
from videbo.misc.task_manager import TaskManager
from videbo.network import NetworkInterfaces
from videbo.hashed_file import HashedFile
from videbo.storage.api.models import RequestFileJWTData
from .api.models import DistributorCopyFileStatus, DistributorStatus
from .copying_file import CopyingVideoFile
from .distributed_file import DistributedVideoFile
from .exceptions import (
    NotEnoughSpace,
    NotSafeToDelete,
    NoSuchFile,
    TooManyWaitingClients,
    UnexpectedFileSize,
)


log = getLogger(__name__)


class DistributorFileController:
    COPY_STATUS_UPDATE_PERIOD: ClassVar[float] = 120.  # seconds
    MAX_WAITING_CLIENTS: ClassVar[int] = 60

    _instance: ClassVar[Optional[DistributorFileController]] = None

    base_path: Path
    http_client: Client
    _copying_files: dict[str, CopyingVideoFile]  # hash -> copying file
    _cached_files: dict[str, DistributedVideoFile]  # hash -> existing file
    _cached_files_total_size: int  # in bytes
    _clients_waiting: int  # number of clients waiting for a file being copied

    def __init__(self) -> None:
        self.base_path = settings.files_path
        self.http_client = Client()
        self._copying_files = {}
        self._cached_files = {}
        self._cached_files_total_size = 0
        self._clients_waiting = 0
        self._load_file_list()

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        cls.get_instance()  # init instance
        yield  # No cleanup necessary

    @classmethod
    def get_instance(cls) -> DistributorFileController:
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _load_file_list(self) -> None:
        """Load information about all files saved on the node."""
        for file_path in self.base_path.glob('**/*'):
            if not file_path.is_file():
                continue
            if file_path.suffix == '.tmp':
                file_path.unlink()
                continue
            # TODO: Handle the case that any unexpected files are found,
            #       possibly by checking valid suffixes and stems.
            file_hash = file_path.stem
            file_size = file_path.stat().st_size
            file = DistributedVideoFile(file_hash, file_path.suffix, file_size)
            self._cached_files[file_hash] = file
            self._cached_files_total_size += file_size
            if len(self._cached_files) < MAX_NUM_FILES_TO_PRINT:
                log.info(f"Found video {file_path}")
        if len(self._cached_files) >= MAX_NUM_FILES_TO_PRINT:
            log.info("Skip logging the other files that were found")
        log.info(f"Found {len(self._cached_files)} videos")

    @property
    def files_total_size_mb(self) -> float:
        return self._cached_files_total_size / MEGA

    @property
    def files_count(self) -> int:
        return len(self._cached_files)

    def iter_files(self) -> Iterator[DistributedVideoFile]:
        return iter(self._cached_files.values())

    def get_path(self, file: HashedFile, temp: bool = False, relative: bool = False) -> Path:
        """
        Returns the path to a video file that may or may not exist on the node.

        Args:
            file: Self-explanatory
            temp: If `True`, the string `.tmp` is appended to the filename.
            relative: If `True` the node's base path is omitted from the start of the path.
        """
        name = str(file)
        if temp:
            name += '.tmp'
        return rel_path(name) if relative else Path(self.base_path, rel_path(name))

    async def file_exists(self, file_hash: str, *, timeout: float = 60.) -> bool:
        """
        Returns `True` if the node controls a file with the given `file_hash`.

        If the node is currently downloading that file, this coroutine will
        wait for the copying to be completed for `timeout` seconds; if the file
        is not downloaded by the end of that, it will return `False`.
        If the node is currently downloading that file, but too many other
        clients are also waiting for the download to be finished, it will _not_
        wait, but return `False` immediately.

        Args:
            file_hash:
                Self-explanatory
            timeout (optional):
                Maximum number of seconds to wait, if the file is being copied.

        Returns:
            Whether the node controls a file with the given `file_hash`.
        """
        try:
            await self.get_file(file_hash, timeout=timeout)
        except (NoSuchFile, TooManyWaitingClients, AsyncTimeoutError):
            return False
        return True

    async def get_file(self, file_hash: str, *, timeout: float = 60.) -> DistributedVideoFile:
        """
        Returns the object representing the file with the given `file_hash`.

        If the node is currently downloading that file, this coroutine will
        wait for the copying to be completed for `timeout` seconds.
        If the node is already fully in possession of the file, the method will
        return immediately.
        If the node is currently downloading that file, but too many other
        clients are also waiting for the download to be finished, this method
        will _not_ wait, but raise a `TooManyWaitingClients` exception.
        If it has no knowledge of a file with the specified `file_hash`, a
        `NoSuchFile` exception will be raised.

        Args:
            file_hash:
                Self-explanatory
            timeout (optional):
                Maximum number of seconds to wait, if the file is being copied.

        Returns:
            Instance of the `DistributedVideoFile` controlled by the node.

        Raises:
            NoSuchFile:
                If no file with such a `file_hash` is controlled by this node.
            TooManyWaitingClients:
                Self-explanatory.
            asyncio.TimeoutError:
                If the file has not finished copying after `timeout` seconds.
        """
        try:
            return self._cached_files[file_hash]
        except KeyError:
            try:
                copying_file = self._copying_files[file_hash]
            except KeyError:
                raise NoSuchFile(file_hash) from None
        if self._clients_waiting >= self.MAX_WAITING_CLIENTS:
            raise TooManyWaitingClients(copying_file, self._clients_waiting)
        self._clients_waiting += 1
        try:
            await copying_file.wait_until_finished(timeout)  # May raise asyncio.TimeoutError
        finally:
            self._clients_waiting -= 1
        # If we did not time-out, the file should now be available.
        return self._cached_files[file_hash]

    async def get_free_space(self) -> float:
        """Returns free space in MB excluding the space that should be empty."""
        free = await get_free_disk_space(str(self.base_path))
        return max(free - settings.distribution.leave_free_space_mb, 0.)

    @staticmethod
    def _log_copy_status(file: CopyingVideoFile, size_mb: float) -> None:
        log.info(
            f"Copied {file.loaded_bytes / MEGA:.1f}/{size_mb:.1f} MB "
            f"({file.loaded_bytes / file.expected_bytes:.1%}) of file {file}"
        )

    async def _download(self, file: CopyingVideoFile, temp_path: Path) -> None:
        free_space = await self.get_free_space()
        size_mb = file.expected_bytes / MEGA
        if size_mb > free_space:
            raise NotEnoughSpace(size_mb, free_space)
        last_update_time = time()
        jwt = RequestFileJWTData.node_default(
            file.hash,
            file.ext,
            expiration_time=int(last_update_time) + 300,
        )
        file_obj = cast(
            FileIO,
            await run_in_default_executor(temp_path.open, 'wb', 0),
        )
        try:
            async for data in self.http_client.request_file_read(
                file.source_url + "/file",
                jwt,
                chunk_size=MEGA,
                timeout=2. * 60 * 60,
            ):
                file.loaded_bytes += len(data)
                await run_in_default_executor(file_obj.write, data)
                if (time() - last_update_time) > self.COPY_STATUS_UPDATE_PERIOD:
                    last_update_time = time()
                    self._log_copy_status(file, size_mb)
        finally:
            await run_in_default_executor(file_obj.close)
        log.info(f"Copied {file} ({size_mb:.1f} MB) from {file.source_url}")

    async def _persist(self, file: CopyingVideoFile, temp_path: Path) -> None:
        if file.loaded_bytes != file.expected_bytes:
            raise UnexpectedFileSize(file)
        # Move the file to its final location (without .tmp suffix).
        await run_in_default_executor(temp_path.rename, self.get_path(file))
        # There must be no context switch by the event loop between marking the
        # file copying as finished and adding it to the controller.
        self._cached_files[file.hash] = file.as_finished()
        self._cached_files_total_size += file.loaded_bytes
        log.debug(f"Saved {file}")

    async def _download_and_persist(self, file: CopyingVideoFile) -> None:
        temp_path = self.get_path(file, True)
        try:
            # ensure dir exists
            await run_in_default_executor(
                temp_path.parent.mkdir, 0o755, True, True
            )
            await self._download(file, temp_path)
            await self._persist(file, temp_path)
        except Exception as e:
            log.exception(
                f"{e.__class__.__name__} copying {file} from {file.source_url}"
            )
        finally:
            del self._copying_files[file.hash]

    def copy_file(
        self,
        file_hash: str,
        file_ext: str,
        from_url: str,
        expected_file_size: int,
    ) -> None:
        if file_hash in self._cached_files:
            log.info(f"File {file_hash}{file_ext} is already here")
            return
        if file_hash in self._copying_files:
            log.info(f"File {file_hash}{file_ext} is already being copied")
            return
        copying_file = CopyingVideoFile(
            file_hash=file_hash,
            file_ext=file_ext,
            source_url=from_url,
            expected_bytes=expected_file_size,
        )
        self._copying_files[file_hash] = copying_file
        log.info(f"Start copying file {copying_file} from {from_url}")
        TaskManager.fire_and_forget(
            self._download_and_persist(copying_file),
            name="copy_file_from_node",
        )

    async def delete_file(self, file_hash: str, safe: bool = True) -> None:
        """
        Deletes file from disk.

        You cannot delete a file that is currently being downloaded.
        TODO: Maybe we can interrupt a download, if we keep a reference to the
              task around.

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
        dist_file = self._cached_files.get(file_hash)
        if dist_file is None:
            raise NoSuchFile(file_hash)
        cutoff_time = time() - settings.dist_last_request_safety_seconds
        if safe and dist_file.last_requested > cutoff_time:
            raise NotSafeToDelete(dist_file)
        del self._cached_files[file_hash]
        self._cached_files_total_size -= dist_file.size
        path = self.get_path(dist_file)
        await run_in_default_executor(path.unlink)
        log.debug(f"Deleted {dist_file}")

    async def get_status(self) -> DistributorStatus:
        status = DistributorStatus(
            tx_current_rate=0,
            rx_current_rate=0,
            tx_total=0,
            rx_total=0,
            tx_max_rate=settings.tx_max_rate_mbit,
            files_total_size=self.files_total_size_mb,
            files_count=self.files_count,
            free_space=await self.get_free_space(),
            bound_to_storage_node_base_url=settings.public_base_url,
            waiting_clients=self._clients_waiting,
            copy_files_status=[],
        )
        now = time()
        for file in self._copying_files.values():
            status.copy_files_status.append(
                DistributorCopyFileStatus(
                    hash=file.hash,
                    file_ext=file.ext,
                    loaded=file.loaded_bytes,
                    file_size=file.expected_bytes,
                    duration=now - file.time_started,
                )
            )
        NetworkInterfaces.get_instance().update_node_status(status, logger=log)
        return status
