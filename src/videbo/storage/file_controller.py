from __future__ import annotations
from asyncio.tasks import gather
from contextlib import suppress
from logging import getLogger
from pathlib import Path
from time import time
from typing import Optional, TYPE_CHECKING

from videbo import settings
from videbo.exceptions import LMSInterfaceError
from videbo.file_controller import FileController
from videbo.lms_api import LMS
from videbo.misc.constants import JPG_EXT
from videbo.misc.functions import get_free_disk_space, move_file, rel_path, run_in_default_executor
from videbo.misc.periodic import Periodic
from videbo.misc.task_manager import TaskManager
from videbo.network import NetworkInterfaces
from .distribution import DistributionController
from .stored_file import StoredVideoFile
from .thumbnail_cache import ThumbnailCache
from .api.models import StorageStatus

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterable


log = getLogger(__name__)


class StorageFileController(FileController[StoredVideoFile]):
    """
    Storage file controller singleton.

    Acts as a read-only mapping of hashes to file instances with additional
    methods for looking up files, storing files temporarily or permanently, and
    deleting files.

    !!! note
        The metaclass will enforce that only a single instance of any subclass
        is ever created. Any further attempts at initialization will return
        the instance constructed the first time and are otherwise no-op.
    """

    _files_dir: Path    # permanent storage
    _temp_dir: Path     # temporary storage
    temp_out_dir: Path  # thumbnail creation

    thumb_memory_cache: ThumbnailCache
    distribution_controller: DistributionController
    num_current_uploads: int

    log = log  # To override the logger name in the parent class' method calls.

    def __init__(self) -> None:
        """
        Initializes and starts relevant periodic tasks.

        Ensures that directories for permanent and temporary storage are set up.
        Schedules `Periodic` tasks for the following functions:
        - `remove_old_temp_files`
        - `discard_old_video_views`

        See `FileController.__init__` for more.
        """
        self._prepare_directories()
        super().__init__()
        self.thumb_memory_cache = ThumbnailCache()
        self.distribution_controller = DistributionController(
            node_urls=settings.distribution.static_node_base_urls,
            http_client=self._http_client,
        )
        self.num_current_uploads = 0
        Periodic(self.remove_old_temp_files)(settings.temp_file_cleanup_freq)
        Periodic(self.discard_old_video_views)(settings.views_update_freq)

    @property
    def files_dir(self) -> Path:
        """The directory where all controlled files reside."""
        return self._files_dir

    @property
    def temp_dir(self) -> Path:
        """The directory where all temporary files are put."""
        return self._temp_dir

    def _prepare_directories(self) -> None:
        """Ensures that the necessary directories exist."""
        self._files_dir = Path(settings.files_path, "storage")
        self._temp_dir = Path(settings.files_path, "temp")
        self.temp_out_dir = Path(self.temp_dir, "out")

        self.files_dir.mkdir(exist_ok=True)
        self.temp_dir.mkdir(exist_ok=True)
        # TODO(daniil-berg): Remove the `temp_out_dir` attribute completely.
        #                    https://github.com/innocampus/videbo/issues/18
        self.temp_out_dir.mkdir(exist_ok=True)
        self.temp_out_dir.chmod(0o777)

    def load_file_predicate(self, file_path: Path) -> bool:
        if file_path.suffix == JPG_EXT:
            return False
        return super().load_file_predicate(file_path)

    def get_path(self, file_name: str, *, temp: bool = False) -> Path:
        """
        Returns the path the controller would use for the specified file name.

        Args:
            file_name:
                Self-explanatory; must have exactly one suffix (dot-separated).
            temp:
                If `True` the path to a temporary file with the specified name
                will be returned.

        Returns:
            The path from which the controller would read a file with the
            specified name. Need not be the canonical or absolute path. At the
            time of calling, the file may or may not exist on the file system.

        Raises:
            ValueError: The `file_name` has no or more than one suffix.
        """
        if temp:
            return Path(self.temp_dir, file_name)
        return Path(self.files_dir, rel_path(file_name))

    def get_thumbnail_path(
        self,
        video_hash: str,
        *,
        num: int,
        temp: bool = False,
    ) -> Path:
        """
        Returns the path the controller would use for a thumbnail.

        Args:
            video_hash:
                The hash of the video the file the thumbnail would belong to.
            num:
                The thumbnail number.
            temp:
                If `True` the path to a temporary file will be returned.

        Returns:
            The path from which the controller would read a thumbnail.
            Need not be the canonical or absolute path. At the time of calling,
            the file may or may not exist on the file system.

        Raises:
            ValueError: The `video_hash` contains a dot.
        """
        return self.get_path(f"{video_hash}_{num}{JPG_EXT}", temp=temp)

    async def filtered_files(
        self,
        orphaned: Optional[bool] = None,
        extensions: Iterable[str] = FileController.VIDEO_EXT_WHITELIST,
    ) -> AsyncIterator[StoredVideoFile]:
        """
        Yields `StoredVideoFile` instances that match the filter criteria.

        All parameters act as filters; if `None` is passed as an argument, no
        filtering is applied with respect to that parameter.

        This method only ever actually awaits, if filtering by orphan status is
        applied; otherwise it returns immediately.

        Args:
            orphaned (optional):
                If `True`, filters out every file known to at least one LMS;
                if `False`, filters out files that no LMS knows about;
                if `None` (default), no filtering by orphan status is applied.
            extensions (optional):
                Iterable of strings representing the file extensions the
                filtered files should have.

        Returns:
            Asynchronous iterator over the permanently stored files that match
            the provided filtering criteria.
        """
        if orphaned is None:
            orphaned_hashes = set()
        else:
            orphaned_hashes = set(
                await LMS.filter_orphaned_videos(
                    *self.iter_files(),
                    client=self._http_client,
                )
            )
        extensions = set(extensions)
        invalid_extensions = extensions.difference(self.VIDEO_EXT_WHITELIST)
        if invalid_extensions:
            raise ValueError(f"Invalid file extension(s): {invalid_extensions}")
        for file in self.iter_files():
            if file.ext not in extensions:
                continue
            if orphaned is True and file.hash not in orphaned_hashes:
                continue
            if orphaned is False and file.hash in orphaned_hashes:
                continue
            yield file

    async def store_file_permanently(self, file_name: str) -> Path:
        """
        Moves specified file from temporary to permanent storage.

        The full paths are calculated by the `get_path` method.

        If the destination directory does not exist yet,
        it is created and its permissions are set to 755.

        If a file already exists at the calculated destination,
        no copying is done and the source file is deleted.

        The operation is done in another thread to avoid blocking.

        Args:
            file_name:
                Self-explanatory; must have exactly one suffix (dot-separated).

        Returns:
            The destination path of the successfully stored file as returned by
            the `get_path` method.

        Raises:
            ValueError:
                The `file_name` has no or more than one suffix.
            FileNotFoundError:
                No temporary file with the specified file name was found.
            OSError:
                Destination file or directory could not be created or the source
                file could not be removed for an unexpected reason.
        """
        source = self.get_path(file_name, temp=True)
        destination = self.get_path(file_name)
        await run_in_default_executor(move_file, source, destination, 0o755)
        return destination

    async def store_permanently(
        self,
        video_file_name: str,
        *,
        thumbnail_count: Optional[int] = None,
    ) -> None:
        """
        Moves video and thumbnails from temporary to permanent storage.

        See `store_file_permanently` for details.

        Args:
            video_file_name:
                Name of the video file to be stored.
            thumbnail_count (optional):
                If omitted or `None` (default), the number of thumbnails
                to store is taken from `settings.thumbnails.suggestion_count`.

        Raises:
            ValueError:
                The `video_file_name` has no or more than one suffix.
            FileNotFoundError:
                No temporary file with the specified file name was found.
            OSError:
                Destination file or directory could not be created or the source
                file could not be removed for an unexpected reason.
        """
        path = await self.store_file_permanently(video_file_name)
        log.info(f"Video stored permanently: {video_file_name}")
        self._add_file(StoredVideoFile.from_path(path))
        thumbnail_count = thumbnail_count or settings.thumbnails.suggestion_count
        coroutines = (
            self.store_file_permanently(f"{path.stem}_{num}{JPG_EXT}")
            for num in range(thumbnail_count)
        )
        # TODO(daniil-berg): Handle the exceptions for storing thumbnails.
        #                    https://github.com/innocampus/videbo/issues/17
        await gather(*coroutines, return_exceptions=True)
        log.info(
            f"Permanently stored {thumbnail_count} thumbnails "
            f"for video {video_file_name}"
        )

    async def remove_video(self, file: StoredVideoFile) -> None:
        """
        Removes the specified video `file` from storage.

        Calls on the distributor nodes to remove the file as well.

        The deletion operation is done in another thread to avoid blocking.

        Args:
            file: Self-explanatory.

        Raises:
            OSError: File could not be removed for an unexpected reason.
        """
        file_path = self.get_path(str(file))
        await run_in_default_executor(file_path.unlink)
        self._remove_file(file.hash)
        self.distribution_controller.remove_from_nodes(file)
        log.info(f"Video removed from storage: {file.hash}")

    async def remove_thumbnails(
        self,
        video_hash: str,
        *,
        count: Optional[int] = None,
    ) -> None:
        """
        Removes the thumbnails for the specified video file from storage.

        The operations are done in another thread to avoid blocking.

        Args:
            video_hash:
                Hash of the video file the thumbnails belong to.
            count:
                If omitted or `None` (default), the number of thumbnails
                to delete is taken from `settings.thumbnails.suggestion_count`.
        """
        count = count or settings.thumbnails.suggestion_count
        coroutines = []
        for num in range(count):
            path = self.get_path(f"{video_hash}_{num}{JPG_EXT}")
            with suppress(KeyError):
                del self.thumb_memory_cache[path]
            coroutines.append(run_in_default_executor(path.unlink))
        # TODO(daniil-berg): Handle the exceptions for deleting thumbnails.
        #                    https://github.com/innocampus/videbo/issues/16
        await gather(*coroutines, return_exceptions=True)
        log.info(f"Removed {count} thumbnails for video {video_hash}")

    async def remove_files(
        self,
        *hashes: str,
        origin: Optional[str] = None,
    ) -> set[str]:
        """
        Tries to remove the files with the specified `*hashes`.

        This method checks all registered LMS for the provided files and removes
        only those that are orphaned.

        Returns the subset of `*hashes` corresponding to those files that were
        _not_ deleted.

        Args:
            *hashes:
                Any number of hashes representing stored files. They should all
                be files that are actually managed by the controller.
            origin (optional):
                If provided a LMS API address, that LMS is _not_ checked.
                This means any file _only_ known to that LMS will be removed.

        Returns:
            Subset of `*hashes` of those files that were _not_ deleted.
        """
        files = [
            self[file_hash]
            for file_hash in hashes
            if file_hash in self
        ]
        log.info(f"{len(files)} files will be checked.")
        hashes_set = set(hashes)
        try:
            orphaned = await LMS.filter_orphaned_videos(
                *files,
                client=self._http_client,
                origin=origin,
            )
        except LMSInterfaceError:
            log.warning("Could not check LMS for files. Not deleting anything.")
            return hashes_set
        log.info(f"{len(orphaned)} files are being deleted.")
        for file_hash in orphaned:
            # It is theoretically possible that in the time between the request
            # to the LMS and now, one of the files is no longer known to the
            # controller. In that case, we would get a `KeyError` here.
            try:
                file = self[file_hash]
            except KeyError:  # pragma: no cover
                log.warning(f"File not controlled by {self}: {file_hash}")
                continue
            # Removing thumbnails should not raise any exceptions.
            await self.remove_thumbnails(file_hash)
            try:
                await self.remove_video(file)
            except OSError as e:
                log.error(f"Unexpected failure to delete video {file}: {e!r}")
                continue
            log.info(f"Video and thumbnails deleted: {file_hash}")
            hashes_set.discard(file_hash)
        return hashes_set

    def schedule_file_removal(
        self,
        file_hash: str,
        file_ext: str,
        origin: Optional[str] = None,
    ) -> None:
        """
        Starts a task to remove the specified file.

        For details see `remove_files`.

        Args:
            file_hash:
                Self-explanatory.
            file_ext:
                Self-explanatory.
            origin:
                See `remove_files`.
        """
        file = self.get(file_hash)
        if file is None or file.ext != file_ext:
            log.warning(f"So such file to remove: {file_hash}{file_ext}")
            return
        log.info(f"Scheduling file deletion: {file_hash}{file_ext}")
        TaskManager.fire_and_forget(
            self.remove_files(file_hash, origin=origin)
        )

    def _remove_old_temp_files(self) -> int:
        """
        Iterates over the temporary files and removes those that are too old.

        The maximum storage time for temporary files is defined in
        `settings.max_temp_storage_hours`.

        Returns:
            Number of deleted temporary files.
        """
        count = 0
        old = time() - 60 * 60 * settings.max_temp_storage_hours
        for file in self.temp_dir.iterdir():
            if file.is_file() and file.stat().st_mtime < old:
                try:
                    file.unlink()
                except OSError as e:  # pragma: no cover
                    log.error(f"Failed to delete temporary file {file}: {e!r}")
                    continue
                count += 1
        return count

    async def remove_old_temp_files(self) -> None:
        """
        Removes temporary files that are too old.

        The maximum storage time for temporary files is defined in
        `settings.max_temp_storage_hours`.

        The operations are done in another thread to avoid blocking.
        """
        count = await run_in_default_executor(self._remove_old_temp_files)
        log.info(f"Cleaned up temp directory: Removed {count} old file(s).")

    async def discard_old_video_views(self) -> None:
        """
        Discards old views for all controlled video files.

        How long views are counted as recent enough is defined in
        `settings.views_retention_seconds`.
        """
        cutoff_timestamp = time() - settings.views_retention_seconds
        for file in self.iter_files():
            file.discard_views_older_than(cutoff_timestamp)

    async def get_status(self) -> StorageStatus:
        """Returns the status of the entire storage node."""
        status = StorageStatus(
            tx_current_rate=0,
            rx_current_rate=0,
            tx_total=0,
            rx_total=0,
            tx_max_rate=settings.tx_max_rate_mbit,
            files_total_size=self.files_total_size_mb,
            files_count=len(self),
            free_space=await get_free_disk_space(settings.files_path),
            distributor_nodes=[
                node.base_url
                for node in self.distribution_controller.iter_nodes()
            ],
            num_current_uploads=self.num_current_uploads,
        )
        NetworkInterfaces.get_instance().update_node_status(status, logger=log)
        return status
