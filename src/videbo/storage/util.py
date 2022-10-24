from __future__ import annotations
import asyncio
import hashlib
import logging
import os
import tempfile
import time
from collections.abc import AsyncIterator, Iterable, Iterator
from pathlib import Path
from typing import BinaryIO, Optional, Union

from aiohttp.web_app import Application

from videbo import settings
from videbo.client import Client
from videbo.exceptions import PendingWriteOperationError, CouldNotCreateDir, LMSInterfaceError
from videbo.lms_api import LMS
from videbo.misc import MEGA
from videbo.misc.functions import get_free_disk_space, rel_path
from videbo.misc.lru_dict import BytesLimitLRU
from videbo.misc.task_manager import TaskManager
from videbo.network import NetworkInterfaces
from videbo.video import VideoInfo, Video, VideoConfig
from .distribution import DistributionController, FileNodes
from .exceptions import HashedFileInvalidExtensionError
from .api.models import FileType, StorageStatus


log = logging.getLogger(__name__)


FILE_EXT_WHITELIST = ('.mp4', '.webm')
JPG_EXT = '.jpg'  # for thumbnails
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

        self._garbage_collector_task = asyncio.create_task(self._garbage_collect_cron())
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

    def get_files_total_size_mb(self) -> int:
        return int(self._cached_files_total_size / 1024 / 1024)

    def get_files_count(self) -> int:
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

    async def get_file(self, file_hash: str, file_ext: str) -> StoredHashedVideoFile:
        """Get video file in storage and check that it really exists."""
        file = self._cached_files.get(file_hash)
        if file and file.file_ext == file_ext:
            return file

        raise FileNotFoundError()

    async def generate_thumbs(self, file: HashedVideoFile, video: VideoInfo) -> int:
        """Generates thumbnail suggestions."""
        video_length = video.get_length()
        thumb_height = settings.thumbnails.height
        thumb_count = settings.thumbnails.suggestion_count
        video_check_user = settings.video.check_user
        # Generate thumbnails concurrently
        tasks = []
        for thumb_nr in range(thumb_count):
            thumb_path = self.get_thumb_path_in_temp(file, thumb_nr)
            offset = int(video_length / thumb_count * (thumb_nr + 0.5))
            temp_out_file = None
            if video_check_user is not None:
                temp_out_file = Path(self.temp_out_dir, file.hash + "_" + str(thumb_nr) + JPG_EXT)
            tasks.append(Video(video_config=VideoConfig()).save_thumbnail(
                video.video_file, thumb_path, offset, thumb_height, temp_output_file=temp_out_file))
        await asyncio.gather(*tasks)
        return thumb_count

    @classmethod
    def get_hash_gen(cls):  # type: ignore
        """Get hashing method that is used for all files in the video."""
        return hashlib.sha256()

    def create_temp_file(self) -> TempFile:
        """Create a space where we can write data to."""
        fd, path = tempfile.mkstemp(prefix='upload_', dir=self.temp_dir)  # actually blocking io
        os.chmod(path, 0o644)  # Make readable for check_user
        return TempFile(os.fdopen(fd, mode='wb'), Path(path), self)

    def get_path(self, file: HashedVideoFile) -> Path:
        """Get path where to find a file with its hash."""
        return Path(self.storage_dir, rel_path(str(file)))

    def get_path_in_temp(self, file: HashedVideoFile) -> Path:
        return TempFile.get_path(self.temp_dir, file)

    def get_thumb_path(self, file: HashedVideoFile, thumb_nr: int) -> Path:
        """Get path where to find a thumbnail with a hash."""
        file_name = file.hash + "_" + str(thumb_nr) + JPG_EXT
        return Path(self.storage_dir, rel_path(file_name))

    def get_thumb_path_in_temp(self, file: HashedVideoFile, thumb_nr: int) -> Path:
        return TempFile.get_thumb_path(self.temp_dir, file, thumb_nr)

    @staticmethod
    def _delete_file(file_path: Path) -> bool:
        # Check source file really exists.
        if not file_path.is_file():
            return False

        file_path.unlink()
        return True

    @staticmethod
    def _move_file(path: Path, new_file_path: Path) -> None:
        # Check source file really exists.
        if not path.is_file():
            raise FileNotFoundError()

        # Ensure dir exists.
        parent = new_file_path.parent
        if not parent.is_dir():
            parent.mkdir(mode=0o755, parents=True)

        if new_file_path.is_file():
            # If a file with the hash already exists, we don't need another copy.
            path.unlink()
        else:
            path.rename(new_file_path)
            new_file_path.chmod(0o644)

    async def add_file_from_temp(self, file: HashedVideoFile) -> None:
        """Add a file to the storage that is currently stored in the temp dir."""
        temp_path = self.get_path_in_temp(file)
        new_file_path = self.get_path(file)

        # Run in another thread as there is blocking io.
        await asyncio.get_event_loop().run_in_executor(None, self._move_file, temp_path, new_file_path)
        log.info("Added file with hash %s permanently to storage.", file.hash)
        self._add_video_to_cache(file.hash, file.file_ext, new_file_path)

    async def add_thumbs_from_temp(self, file: HashedVideoFile, thumb_count: int) -> None:
        """Add thumbnails to the video that are currently stored in the temp dir."""
        tasks = []
        for thumb_nr in range(thumb_count):
            old_thumb_path = self.get_thumb_path_in_temp(file, thumb_nr)
            new_thumb_file = self.get_thumb_path(file, thumb_nr)

            # Run in another thread as there is blocking io.
            tasks.append(asyncio.get_event_loop().run_in_executor(None, self._move_file, old_thumb_path,
                                                                  new_thumb_file))
        await asyncio.gather(*tasks)
        log.info(f"Added {thumb_count} thumbnails for file with hash {file.hash} permanently to storage.")

    async def remove(self, file: StoredHashedVideoFile) -> None:
        file_path = self.get_path(file)

        # Run in another thread as there is blocking io.
        if not await asyncio.get_event_loop().run_in_executor(None, self._delete_file, file_path):
            raise FileNotFoundError()

        # Remove file from cached files and delete all copies on distributor nodes.
        self._cached_files.pop(file.hash)
        self._cached_files_total_size -= file.file_size
        self.distribution_controller.remove_video(file)

        log.info(f"Removed file with hash {file.hash} permanently from storage.")

    async def remove_thumbs(self, file: HashedVideoFile) -> None:
        thumb_nr = 0
        # Remove increasing thumbnail ids until file not found
        while True:
            thumb_path = self.get_thumb_path(file, thumb_nr)
            try:
                del self.thumb_memory_cache[thumb_path]
            except KeyError:
                pass

            # Run in another thread as there is blocking io.
            if not await asyncio.get_event_loop().run_in_executor(None, self._delete_file, thumb_path):
                break
            thumb_nr += 1

        log.info(f"Removed {thumb_nr} thumbnails for file with hash {file.hash} permanently from storage.")

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
            await self.remove_thumbs(file)
            await self.remove(file)
            log.info(f"Deleted video {video.hash} permanently")
            hashes_set.discard(video.hash)
        return hashes_set

    def garbage_collect_temp_dir(self) -> int:
        """Delete files older than GC_TEMP_FILES_SECS."""
        count = 0
        old = time.time() - self.GC_TEMP_FILES_SECS
        for file in self.temp_dir.iterdir():
            if file.is_file() and file.stat().st_mtime < old:
                # File is old and most likely not needed anymore.
                file.unlink()
                count += 1

        return count

    async def _garbage_collect_cron(self) -> None:
        """Endless loop that cleans up data periodically."""
        while True:
            files_deleted = await asyncio.get_event_loop().run_in_executor(None, self.garbage_collect_temp_dir)
            if files_deleted > 0:
                log.info(f"Run GC of temp folder: Removed {files_deleted} file(s).")

            await asyncio.sleep(self.GC_ITERATION_SECS)

    async def get_status(self) -> StorageStatus:
        status = StorageStatus.construct()
        # Same attributes for storage and distributor nodes:
        status.files_total_size = self.get_files_total_size_mb()
        status.files_count = self.get_files_count()
        status.free_space = await get_free_disk_space(str(settings.files_path))
        status.tx_max_rate = settings.tx_max_rate_mbit
        NetworkInterfaces.get_instance().update_node_status(status, logger=log)
        # Specific to storage node:
        status.distributor_nodes = self.distribution_controller.get_dist_node_base_urls()
        status.num_current_uploads = self.num_current_uploads
        return status


class TempFile:
    """Used to handle files that are getting uploaded right now or were just uploaded, but not yet added to the
    file storage finally.
    """

    def __init__(self, file: BinaryIO, path: Path, storage: FileStorage):
        self.hash = FileStorage.get_hash_gen()  # type: ignore[no-untyped-call]
        self.file = file
        self.path: Path = path
        self.storage = storage
        self.size = 0
        self.is_writing = False

    async def write(self, data: bytes) -> None:
        """Write data to the file and compute the hash on the fly."""
        if self.is_writing:
            raise PendingWriteOperationError()
        self.size += len(data)
        self.is_writing = True
        # Run in another thread as there is blocking io.
        await asyncio.get_event_loop().run_in_executor(None, self._update_hash_write_file, data)
        self.is_writing = False

    def _update_hash_write_file(self, data: bytes) -> None:
        self.hash.update(data)
        self.file.write(data)

    async def close(self) -> None:
        await asyncio.get_event_loop().run_in_executor(None, self.file.close)

    async def persist(self, file_ext: str) -> HashedVideoFile:
        """
        Close file and name the file after its hash.
        :param file_ext:
        :return: The file hash.
        """
        if self.is_writing:
            raise PendingWriteOperationError()
        self.is_writing = True  # Don't allow any additional writes.
        file = HashedVideoFile(self.hash.hexdigest(), file_ext)
        # Run in another thread as there is blocking io.
        await asyncio.get_event_loop().run_in_executor(None, self._move, file)
        return file

    def _move(self, file: HashedVideoFile) -> None:
        new_path = self.get_path(self.storage.temp_dir, file)
        if new_path.is_file():
            # If a file with the hash already exists, we don't need another copy.
            self.path.unlink()
        else:
            self.path.rename(new_path)

    @classmethod
    def get_path(cls, temp_dir: Path, file: HashedVideoFile) -> Path:
        return Path(temp_dir, file.hash + file.file_ext)

    @classmethod
    def get_thumb_path(cls, temp_dir: Path, file: HashedVideoFile, thumb_nr: int) -> Path:
        file_name = file.hash + "_" + str(thumb_nr) + JPG_EXT
        return Path(temp_dir, file_name)

    async def delete(self) -> None:
        await asyncio.get_event_loop().run_in_executor(None, self._delete_file)

    def _delete_file(self) -> None:
        self.file.close()
        if self.path.is_file():
            self.path.unlink()


def is_allowed_file_ending(filename: Optional[str]) -> bool:
    """Simple check that the file ending is on the whitelist."""
    if filename is None:
        return True
    return filename.lower().endswith(FILE_EXT_WHITELIST)


def schedule_video_delete(file_hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    log.info(f"Delete video with hash {file_hash}")
    TaskManager.fire_and_forget_task(asyncio.create_task(_video_delete_task(file_hash, file_ext, origin)))


async def _video_delete_task(file_hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    file_storage = FileStorage.get_instance()
    try:
        file = await file_storage.get_file(file_hash, file_ext)
    except FileNotFoundError:
        log.info(f"Video delete: file not found: {file_hash}{file_ext}.")
        return
    await file_storage.remove_files(file.hash, origin=origin)
