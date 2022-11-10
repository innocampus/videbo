from __future__ import annotations
import logging
from asyncio import sleep as async_sleep
from asyncio.tasks import create_task, gather
from collections.abc import AsyncIterator, Iterable, Iterator
from pathlib import Path
from time import time
from typing import Optional, Union

from aiohttp.web_app import Application

from videbo import settings
from videbo.client import Client
from videbo.exceptions import CouldNotCreateDir, LMSInterfaceError
from videbo.lms_api import LMS
from videbo.misc import JPG_EXT, MEGA
from videbo.misc.functions import get_free_disk_space, move_file, rel_path, run_in_default_executor
from videbo.misc.lru_dict import BytesLimitLRU
from videbo.misc.task_manager import TaskManager
from videbo.network import NetworkInterfaces
from .distribution import DistributionController, FileNodes
from .exceptions import HashedFileInvalidExtensionError
from .api.models import FileType, StorageStatus


log = logging.getLogger(__name__)


FILE_EXT_WHITELIST = ('.mp4', '.webm')
VALID_EXTENSIONS = frozenset(FILE_EXT_WHITELIST + (JPG_EXT,))


class HashedVideoFile:
    __slots__ = 'hash', 'file_ext'

    hash: str
    file_ext: str

    def __init__(self, file_hash: str, file_ext: str) -> None:
        self.hash = file_hash
        self.file_ext = file_ext

        # Extension has to start with a dot.
        if file_ext[0] != '.':
            raise HashedFileInvalidExtensionError(file_ext)

    # TODO: Add __repr__
    def __str__(self) -> str:
        return self.hash + self.file_ext

    def __hash__(self) -> int:
        return int(self.hash, 16)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HashedVideoFile) or type(self) is not type(other):
            return NotImplemented
        return self.hash == other.hash


class StoredHashedVideoFile(HashedVideoFile):
    __slots__ = 'file_size', 'views', 'nodes'

    file_size: int  # in bytes
    views: int
    nodes: FileNodes

    def __init__(self, file_hash: str, file_ext: str) -> None:
        super().__init__(file_hash, file_ext)
        self.file_size = -1
        self.views = 0
        self.nodes = FileNodes()

    def __lt__(self, other: StoredHashedVideoFile) -> bool:
        """Compare videos by their view counters."""
        return self.views < other.views


_FilesDict = dict[str, HashedVideoFile]
_StoredFilesDict = dict[str, StoredHashedVideoFile]


# TODO: Check the necessity for this function and move it to misc.functions
def create_dir_if_not_exists(path: Union[Path, str], mode: int = 0o777, explicit_chmod: bool = False) -> None:
    path = Path(path)
    if path.is_dir():
        return
    path.mkdir(mode=mode)
    if explicit_chmod:
        path.chmod(mode)
    if not path.is_dir():
        raise CouldNotCreateDir(str(path))
    log.info(f"Created {path}")


class FileStorage:
    """Manages all stored files with their hashes as file names."""
    _instance: Optional[FileStorage] = None

    # garbage collector
    GC_TEMP_FILES_SECS = 12 * 3600
    GC_ITERATION_SECS = 3600  # run gc every n secs

    def __init__(self, path: Path):
        if not path.is_dir():
            log.fatal(f"videos dir {path} does not exist")
            raise NotADirectoryError(path)

        self.path: Path = path
        self.storage_dir: Path = Path(self.path, "storage")
        self.temp_dir: Path = Path(self.path, "temp")
        self.temp_out_dir: Path = Path(self.temp_dir, "out")
        self._cached_files: _StoredFilesDict = {}  # map hashes to files
        self._cached_files_total_size: int = 0  # in bytes
        self.num_current_uploads: int = 0
        self.thumb_memory_cache = BytesLimitLRU(settings.thumbnails.cache_max_mb * MEGA)
        self.http_client: Client = Client()
        self.distribution_controller: DistributionController = DistributionController(http_client=self.http_client)

        create_dir_if_not_exists(self.temp_dir, 0o755)
        create_dir_if_not_exists(self.temp_out_dir, 0o777, explicit_chmod=True)

        self._garbage_collector_task = create_task(self._garbage_collect_cron())
        TaskManager.fire_and_forget_task(self._garbage_collector_task)

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        log.info("Initializing file storage instance...")
        cls.get_instance()  # init instance
        yield  # No cleanup necessary

    @classmethod
    def get_instance(cls) -> FileStorage:
        if cls._instance is None:
            cls._instance = FileStorage(settings.files_path)
            cls._instance._load_file_list()
            for url in settings.distribution.static_node_base_urls:
                cls._instance.distribution_controller.add_new_dist_node(url)
            cls._instance.distribution_controller.start_periodic_reset_task()
        return cls._instance

    def _load_file_list(self) -> None:
        """Load all video files in memory"""
        try:
            for obj in self.storage_dir.glob('**/*'):
                if obj.is_file():
                    file_split = obj.name.split('.')
                    if len(file_split) == 2:
                        file_hash = file_split[0]
                        file_ext = "." + file_split[1]
                        if file_ext in FILE_EXT_WHITELIST:
                            f = self._add_video_to_cache(file_hash, file_ext, obj)
                            if len(self._cached_files) > 20:
                                continue
                            elif len(self._cached_files) == 20:
                                log.info("Skip logging the other files that were found")
                            else:
                                log.info(f"Found video {f}")
        except Exception as e:
            log.exception(f"{str(e)} in load_file_list")
        log.info(f"Found {len(self._cached_files)} videos in storage")

    def _add_video_to_cache(self, file_hash: str, file_ext: str, file_path: Path) -> StoredHashedVideoFile:
        file = StoredHashedVideoFile(file_hash, file_ext)
        try:
            stat = file_path.stat()
        except FileNotFoundError:
            log.exception("FileNotFoundError in _add_video_to_cache")
            return file
        file.file_size = stat.st_size
        self._cached_files[file_hash] = file
        self._cached_files_total_size += file.file_size
        self.distribution_controller.add_video(file)
        return file

    @property
    def files_total_size_mb(self) -> float:
        return self._cached_files_total_size / MEGA

    @property
    def files_count(self) -> int:
        return len(self._cached_files)

    def iter_files(self) -> Iterator[StoredHashedVideoFile]:
        return iter(self._cached_files.values())

    # TODO: Allow actually passing `None` for any filter parameter
    async def filtered_files(self, orphaned: Optional[bool] = None, extensions: Iterable[str] = VALID_EXTENSIONS,
                             types: Iterable[str] = FileType.values()) -> AsyncIterator[StoredHashedVideoFile]:
        """
        Yields `StoredHashedVideoFile` instances that match the filter criteria.

        All parameters act as filters;
        if `None` is passed as an argument, no filtering is applied with respect to that parameter.

        This method only ever actually awaits, if filtering by orphan status is applied.

        Args:
            orphaned (optional):
                If True, filters out every file that at least one LMS knows about;
                conversely, if False, filters out files that no LMS knows about.
            extensions (optional):
                Iterable of strings representing the file extensions the filtered files should have
            types (optional):
                Iterable of strings representing the FileType members the filtered files should correspond to
        """
        if orphaned is None:
            matching_orphan_status = set(self._cached_files.values())
        else:
            matching_orphan_status = await self._filter_by_orphan_status(self._cached_files.values(), orphaned)

        extensions = set(extensions)
        invalid_extensions = extensions.difference(VALID_EXTENSIONS)
        if invalid_extensions:
            raise ValueError(f"Invalid file extension(s): {invalid_extensions}")

        types = set(types)
        invalid_types = types.difference(FileType.values())
        if invalid_types:
            raise ValueError(f"Invalid file type(s): {invalid_types}")

        for file in self._cached_files.values():
            if file.file_ext not in extensions:
                continue
            if file not in matching_orphan_status:
                continue
            # TODO: Filter by file type; this is a placeholder:
            assert types is not None
            yield file

    async def _filter_by_orphan_status(self, files: Iterable[StoredHashedVideoFile], orphaned: bool
                                       ) -> set[StoredHashedVideoFile]:
        """
        Checks all registered LMS for the orphan status of the provided `files`.

        Args:
            files:
                An iterable of `StoredHashedVideoFile` objects.
                They should all be files that are actually managed by the `FileStorage`.
            orphaned:
                If `True` the method returns only the subset of `files` that are orphaned;
                if `False` the method returns only the subset of `files` that are not orphaned.

        Returns:
            Subset of the provided `files` that are managed by the `FileStorage` and match the desired `orphan` status.
        """
        videos = await LMS.filter_orphaned_videos(*files, client=self.http_client)
        orphaned_files = set()
        for video in videos:
            try:
                orphaned_files.add(self._cached_files[video.hash])
            except KeyError:
                log.warning(f"File not managed by this storage node: {video.hash}")
        if orphaned:
            return orphaned_files
        return set(files).difference(orphaned_files)

    def get_file(self, file_hash: str, file_ext: str) -> StoredHashedVideoFile:
        """
        Returns stored video file with the specified hash and extension.

        If no matching file is found in cache, raises `FileNotFoundError`.
        """
        file = self._cached_files.get(file_hash)
        if file is None or file.file_ext != file_ext:
            raise FileNotFoundError
        return file

    def get_path(self, file_name: str, *, temp: bool) -> Path:
        """Constructs standardized storage path for a specified file name."""
        if temp:
            return Path(self.temp_dir, file_name)
        return Path(self.storage_dir, rel_path(file_name))

    def get_perm_video_path(self, file_hash: str, file_ext: str) -> Path:
        """
        Returns permanent storage path for the specified video file.

        If no matching video is found in cache, raises `FileNotFoundError`.
        """
        file = self.get_file(file_hash, file_ext)
        return self.get_path(str(file), temp=False)

    def get_temp_video_path(self, file_hash: str, file_ext: str) -> Path:
        """Returns temporary storage path for the specified video file."""
        return self.get_path(file_hash + file_ext, temp=True)

    def get_perm_thumbnail_path(
        self,
        video_hash: str,
        video_ext: str,
        *,
        num: int,
    ) -> Path:
        """
        Returns permanent thumbnail path for the specified video file.

        If no matching video is found in cache, raises `FileNotFoundError`.
        """
        _ = self.get_file(video_hash, video_ext)
        return self.get_path(f"{video_hash}_{num}{JPG_EXT}", temp=False)

    def get_temp_thumbnail_path(self, video_hash: str, *, num: int) -> Path:
        """Returns temporary thumbnail path for the specified video file."""
        return self.get_path(f"{video_hash}_{num}{JPG_EXT}", temp=True)

    async def store_file_permanently(self, file_name: str) -> Path:
        """
        Moves specified file from temporary to permanent storage.

        The full paths are calculated by the `get_path` method.

        If the destination directory does not exist yet,
        it is created and its permissions are set to 755.

        If a file already exists at the calculated destination,
        no copying is done and the source file is deleted.

        The operation is done in another thread to avoid blocking.
        """
        source = self.get_path(file_name, temp=True)
        destination = self.get_path(file_name, temp=False)
        await run_in_default_executor(move_file, source, destination, 0o755)
        return destination

    async def store_permanently(
        self,
        video_hash: str,
        video_ext: str,
        *,
        thumbnail_count: Optional[int] = None
    ) -> None:
        """
        Moves video and thumbnails from temporary to permanent storage.

        See `store_file_permanently` for details.

        Args:
            video_hash:
                The hash digest of the video file to be stored
            video_ext:
                The file extension of the video to be stored
            thumbnail_count (optional):
                If omitted or `None` (default), the number of thumbnails
                to store is taken from `settings.thumbnails.suggestion_count`.
        """
        path = await self.store_file_permanently(video_hash + video_ext)
        log.info(f"Video stored permanently: {video_hash}")
        self._add_video_to_cache(video_hash, video_ext, path)
        thumbnail_count = thumbnail_count or settings.thumbnails.suggestion_count
        coroutines = (
            self.store_file_permanently(f"{video_hash}_{num}{JPG_EXT}")
            for num in range(thumbnail_count)
        )
        await gather(*coroutines)
        log.info(
            f"Permanently stored {thumbnail_count} thumbnails "
            f"for video {video_hash}"
        )

    async def remove_video(self, file: StoredHashedVideoFile) -> None:
        """
        Removes the specified video file from storage.

        Calls on the distributor nodes to remove the file as well.
        Updates the total files size.

        The operation is done in another thread to avoid blocking.
        """
        file_path = self.get_perm_video_path(file.hash, file.file_ext)
        await run_in_default_executor(file_path.unlink)
        self._cached_files.pop(file.hash)
        self._cached_files_total_size -= file.file_size
        self.distribution_controller.remove_video(file)
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
        """
        count = count or settings.thumbnails.suggestion_count
        coroutines = []
        for num in range(count):
            path = self.get_path(f"{video_hash}_{num}{JPG_EXT}", temp=False)
            try:
                del self.thumb_memory_cache[path]
            except KeyError:
                pass
            coroutines.append(run_in_default_executor(path.unlink))
        await gather(*coroutines)
        log.info(f"Removed {count} thumbnails for video {video_hash}")

    async def remove_files(self, *hashes: str, origin: Optional[str] = None) -> set[str]:
        """
        Checks all registered LMS for the provided files and removes those that are orphaned.

        Returns the subset of `*hashes` corresponding to those files that were _not_ deleted.

        Args:
            *hashes:
                Any number of hashes representing stored files.
                They should all be files that are actually managed by the `FileStorage`.
            origin (optional):
                If provided a LMS API address, that LMS is _not_ checked.
                This means any file _only_ known to that LMS will be removed.

        Returns:
            Subset of `*hashes` corresponding to those files that were _not_ deleted.
        """
        files = []
        for file_hash in hashes:
            try:
                files.append(self._cached_files[file_hash])
            except KeyError:
                log.warning(f"File not managed by this storage node: {file_hash}")
        log.info(f"{len(files)} files will be checked.")
        try:
            orphaned = await LMS.filter_orphaned_videos(*files, client=self.http_client, origin=origin)
        except LMSInterfaceError:
            log.warning("Could not check all LMS for files. Not deleting anything.")
            return set()
        hashes_set = set(hashes)
        log.info(f"{len(orphaned)} files are being deleted.")
        for video in orphaned:
            file = self._cached_files[video.hash]
            await self.remove_thumbnails(video.hash)
            await self.remove_video(file)
            log.info(f"Deleted video {video.hash} permanently")
            hashes_set.discard(video.hash)
        return hashes_set

    def garbage_collect_temp_dir(self) -> int:
        """Delete files older than GC_TEMP_FILES_SECS."""
        count = 0
        old = time() - self.GC_TEMP_FILES_SECS
        for file in self.temp_dir.iterdir():
            if file.is_file() and file.stat().st_mtime < old:
                # File is old and most likely not needed anymore.
                file.unlink()
                count += 1
        return count

    async def _garbage_collect_cron(self) -> None:
        """Endless loop that cleans up data periodically."""
        while True:
            files_deleted = await run_in_default_executor(self.garbage_collect_temp_dir)
            log.info(f"Run GC of temp folder: Removed {files_deleted} file(s).")
            await async_sleep(self.GC_ITERATION_SECS)

    async def get_status(self) -> StorageStatus:
        status = StorageStatus.construct()
        # Same attributes for storage and distributor nodes:
        status.files_total_size = self.files_total_size_mb
        status.files_count = self.files_count
        status.free_space = await get_free_disk_space(str(settings.files_path))
        status.tx_max_rate = settings.tx_max_rate_mbit
        NetworkInterfaces.get_instance().update_node_status(status, logger=log)
        # Specific to storage node:
        status.distributor_nodes = self.distribution_controller.get_dist_node_base_urls()
        status.num_current_uploads = self.num_current_uploads
        return status


def is_allowed_file_ending(filename: Optional[str]) -> bool:
    """Simple check that the file ending is on the whitelist."""
    if filename is None:
        return True
    return filename.lower().endswith(FILE_EXT_WHITELIST)


def schedule_video_delete(file_hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    log.info(f"Delete video with hash {file_hash}")
    TaskManager.fire_and_forget_task(create_task(_video_delete_task(file_hash, file_ext, origin)))


async def _video_delete_task(file_hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    file_storage = FileStorage.get_instance()
    try:
        file = file_storage.get_file(file_hash, file_ext)
    except FileNotFoundError:
        log.info(f"Video delete: file not found: {file_hash}{file_ext}.")
        return
    await file_storage.remove_files(file.hash, origin=origin)
