from __future__ import annotations
from logging import getLogger
from pathlib import Path
from time import time
from typing import ClassVar

from videbo import settings
from videbo.file_controller import FileController
from videbo.hashed_file import HashedFile
from videbo.misc.constants import MEGA
from videbo.misc.functions import (
    get_free_disk_space,
    rel_path,
    run_in_default_executor,
)
from videbo.misc.task_manager import TaskManager
from videbo.network import NetworkInterfaces
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


class DistributorFileController(FileController[DistributedVideoFile]):
    COPY_STATUS_UPDATE_PERIOD: ClassVar[float] = 120.  # seconds
    MAX_WAITING_CLIENTS: ClassVar[int] = 60

    _copying_files: dict[str, CopyingVideoFile]  # hash -> copying file
    _clients_waiting: int  # number of clients waiting for a file being copied

    log = log  # To override the logger name in the parent class' method calls.

    def __init__(self) -> None:
        """
        Initializes the necessary internal attributes.

        See `FileController.__init__` for more.
        """
        super().__init__()
        self._copying_files = {}
        self._clients_waiting = 0

    @property
    def files_dir(self) -> Path:
        return settings.files_path

    def load_file_predicate(self, file_path: Path) -> bool:
        if file_path.suffix == ".tmp":
            log.debug(f"Removing temporary file: {file_path}")
            file_path.unlink()
            return False
        return super().load_file_predicate(file_path)

    def get_path(
        self,
        file_name: str | HashedFile,
        *,
        temp: bool = False,
    ) -> Path:
        if isinstance(file_name, HashedFile):
            file_name = str(file_name)
        if temp:
            file_name += ".tmp"
        return Path(self.files_dir, rel_path(file_name))

    async def get_file(
        self,
        file_hash: str,
        *,
        timeout: float = 60.,
    ) -> DistributedVideoFile:
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
            timeout:
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
            return self[file_hash]
        except KeyError:
            try:
                copying_file = self._copying_files[file_hash]
            except KeyError:
                raise NoSuchFile(file_hash) from None
        if self._clients_waiting >= self.MAX_WAITING_CLIENTS:
            raise TooManyWaitingClients(copying_file, self._clients_waiting)
        self._clients_waiting += 1
        try:
            # May raise `asyncio.TimeoutError`:
            await copying_file.wait_until_finished(timeout)
        finally:
            self._clients_waiting -= 1
        # If we did not time-out, the file should now be available.
        return self[file_hash]

    async def get_free_space(self) -> float:
        """
        Returns free space in MB excluding the space that should be empty.

        Raises:
            FileNotFoundError: The files directory does not exist.
        """
        free = await get_free_disk_space(self.files_dir)
        return max(free - settings.distribution.leave_free_space_mb, 0.)

    @staticmethod
    def _log_copy_status(file: CopyingVideoFile, size_mb: float) -> None:
        """Convenience method for the `_download` loop."""
        log.info(
            f"Copied {file.loaded_bytes / MEGA:.1f}/{size_mb:.1f} MB "
            f"({file.loaded_bytes / file.size:.1%}) of file {file}"
        )

    async def _download_and_persist(self, file: CopyingVideoFile) -> None:
        """
        Downloads and saves the `file`.

        Args:
            file: Represents the file to be copied to the distributor node.

        Raises:
            FileNotFoundError:
                The files directory does not exist.
            NotEnoughSpace:
                There is not enough space in the files directory to save the
                `file` and honor the free space setting.
            UnexpectedFileSize:
                The `loaded_bytes` of the file are not equal to its `size`.
            OSError:
                The destination directory did not yet exist and could not be
                created or writing to the temporary file failed or the fully
                downloaded file could not be moved to its persistent location.
        """
        free_space = await self.get_free_space()
        size_mb = file.size / MEGA
        if size_mb > free_space:
            raise NotEnoughSpace(size_mb, free_space)
        temp_path = self.get_path(file, temp=True)
        final_path = self.get_path(file)
        # Ensure directory exists.
        for dir_ in (temp_path.parent, final_path.parent):
            await run_in_default_executor(dir_.mkdir, 0o755, True, True)
        jwt = RequestFileJWTData.node_default(
            file.hash,
            file.ext,
            expiration_time=int(time()) + 300,
        )
        last_update_time = 0.
        file_obj = await run_in_default_executor(temp_path.open, 'wb', 0)
        try:
            async for data in self._http_client.request_file_read(
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
        finally:  # pragma: no cover
            await run_in_default_executor(file_obj.close)
        log.info(f"Copied {file} ({size_mb:.1f} MB) from {file.source_url}")
        if file.loaded_bytes != file.size:
            raise UnexpectedFileSize(file)
        # Move the file to its final location.
        await run_in_default_executor(temp_path.rename, final_path)
        # There must be no context switch by the event loop between marking the
        # file copying as finished and adding it to the controller.
        self._add_file(file.as_finished())
        log.debug(f"Saved {file}")

    async def download_and_persist(self, file: CopyingVideoFile) -> None:
        """
        Starts copying a file from another node and blocks until it is finished.

        If any `Exception` occurs during the copying process, that error will be
        caught and logged. Only errors caused by invalid file attributes will be
        propagated.

        Args:
            file: Represents the file to be copied to the distributor node.
        """
        try:
            await self._download_and_persist(file)
        except Exception as e:
            # TODO(daniil-berg): Delete the file.
            #                    https://github.com/innocampus/videbo/issues/26
            log.exception(
                f"{e.__class__.__name__} copying {file} from {file.source_url}"
            )
        finally:
            self._copying_files.pop(file.hash, None)

    def schedule_copying(
        self,
        file_hash: str,
        file_ext: str,
        from_url: str,
        expected_file_size: int,
    ) -> CopyingVideoFile | None:
        """
        Starts a background task to copy a file from another node.

        If the file is already controlled or being downloaded nothing happens.

        Args:
            file_hash:
                Hash of the file to be copied.
            file_ext:
                Extension of the file to be copied.
            from_url:
                Base URL of the node to use as a source for the file.
            expected_file_size:
                Exact size the file is expected to have.

        Returns:
            Instance of a `CopyingVideoFile`, if the copying process was
            started successfully or a file with the specified hash, was already
            being downloaded. `None`, if a file with the specified hash is
            already controlled and available.

        Raises:
            ValueError: The file attributes are wrong.
                        TODO: Use custom exception in base `HashedFile` class.
        """
        if file_hash in self:
            log.info(f"File {file_hash}{file_ext} is already here")
            return None
        if file_hash in self._copying_files:
            log.info(f"File {file_hash}{file_ext} is already being copied")
            return self._copying_files[file_hash]
        file = CopyingVideoFile(
            file_hash=file_hash,
            file_ext=file_ext,
            file_size=expected_file_size,
            source_url=from_url,
        )
        log.info(f"Start copying file {file} from {from_url}")
        self._copying_files[file_hash] = file
        TaskManager.fire_and_forget(
            self.download_and_persist(file),
            name="copy_file_from_node",
        )
        return file

    async def delete_file(self, file_hash: str, safe: bool = True) -> None:
        """
        Deletes file from disk.

        You cannot delete a file that is currently being downloaded.
        TODO: Maybe we can interrupt a download, if we keep a reference to the
              task around.

        Args:
            file_hash:
                Self-explanatory
            safe:
                If `True`, safety period from config is checked before deletion.

        Raises:
            NoSuchFile:
                No file with a hash like this is controlled by this node.
            NotSafeToDelete:
                `safe` is `True` and the file in question was last requested
                within the configured safety interval.
            OSError:
                Failure to delete the file.

        Returns:
            `None` upon successful deletion
        """
        dist_file = self.get(file_hash)
        if dist_file is None:
            raise NoSuchFile(file_hash)
        cutoff_time = time() - settings.dist_last_request_safety_seconds
        if safe and dist_file.last_requested > cutoff_time:
            raise NotSafeToDelete(dist_file)
        self._remove_file(file_hash)
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
            files_count=len(self),
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
                    file_size=file.size,
                    duration=now - file.time_started,
                )
            )
        NetworkInterfaces.get_instance().update_node_status(status, logger=log)
        return status
