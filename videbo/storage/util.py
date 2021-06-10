import asyncio
import hashlib
import os
import tempfile
import time
from _sha256 import SHA256Type
from pathlib import Path
from copy import deepcopy
from typing import Optional, Union, Dict, BinaryIO, List, Iterable, Tuple

from videbo.misc import TaskManager, BytesLimitLRU, gather_in_batches, rel_path
from videbo.lms_api import LMSSitesCollection, LMSAPIError
from videbo.video import VideoInfo
from videbo.video import Video
from videbo.video import VideoConfig
from videbo.exceptions import PendingWriteOperationError

from . import storage_logger
from . import storage_settings
from .distribution import DistributionController, FileNodes
from .exceptions import HashedFileInvalidExtensionError, CouldNotCreateTempDir
from .api.models import FileType

FILE_EXT_WHITELIST = ('.mp4', '.webm')
JPG_EXT = '.jpg'  # for thumbnails
VALID_EXTENSIONS = frozenset(FILE_EXT_WHITELIST + (JPG_EXT,))


class HashedVideoFile:
    __slots__ = "hash", "file_extension"

    def __init__(self, file_hash: str, file_extension: str):
        self.hash = file_hash
        self.file_extension = file_extension

        # Extension has to start with a dot.
        if file_extension[0] != ".":
            raise HashedFileInvalidExtensionError(file_extension)

    def __str__(self):
        return self.hash + self.file_extension


class StoredHashedVideoFile(HashedVideoFile):
    __slots__ = "file_size", "views", "nodes", "_dist_initiated"

    def __init__(self, file_hash: str, file_extension: str):
        super().__init__(file_hash, file_extension)
        self.file_size: int = -1  # in bytes
        self.views: int = 0
        self.nodes: FileNodes = FileNodes()
        self._dist_initiated: Optional[Tuple[float, str]] = None
        # When a video file is requested from the storage node, and this triggers the *first* distribution of that file,
        # i.e. copying it to a distributor node, the attribute `._dist_initiated` is set to a 2-tuple with the first
        # element being the timestamp of the moment the copying was initiated and the second element being the string
        # identifier of the user (as contained in `RequestFileJWTData.rid`), whose request triggered the copying task.
        # This is part of a work-around to prevent redirects from a storage node to a distributor node for that same
        # user in the middle of streaming (e.g. by skipping to an unbuffered section of the video), which causes some
        # sort of network error to be raised on the client's side in many scenarios.
        # The timestamp is used to decide whether enough time has passed to assume a new streaming session is taking
        # place and it is safe to redirect.

    def init_dist_by(self, user_rid: str) -> None:
        """Registers that the *first* time distribution of this video file was triggered by a user."""
        self._dist_initiated = time.time(), user_rid

    def prevent_redirect(self, rid: str) -> bool:
        """
        Checks the current time and the `rid` argument against the `._dist_initiated` attribute.
        Returns True only if initial distribution was triggered by a user with the same `rid` and not too long ago.
        """
        if self._dist_initiated is None:
            return False
        init_time, init_rid = self._dist_initiated
        if init_rid != rid:
            return False
        if time.time() > init_time + 3600 * storage_settings.dist_redirect_prevent_hours:
            self._dist_initiated = None
            return False
        return True

    def __lt__(self, other: "StoredHashedVideoFile"):
        """Compare videos by their view counters."""
        return self.views < other.views


_FilesDict = Dict[str, HashedVideoFile]
_StoredFilesDict = Dict[str, StoredHashedVideoFile]


def create_dir_if_not_exists(path: Union[Path, str], mode: int = 0o777, explicit_chmod: bool = False) -> None:
    path = Path(path)
    if path.is_dir():
        return
    path.mkdir(mode=mode)
    if explicit_chmod:
        path.chmod(mode)
    if not path.is_dir():
        raise CouldNotCreateTempDir(path)
    storage_logger.info(f"Created {path}")


class FileStorage:
    """Manages all stored files with their hashes as file names."""
    _instance: Optional["FileStorage"] = None

    # garbage collector
    GC_TEMP_FILES_SECS = 12 * 3600
    GC_ITERATION_SECS = 3600  # run gc every n secs

    def __init__(self, path: Path):
        if not path.is_dir():
            storage_logger.fatal(f"videos dir {path} does not exist")
            raise NotADirectoryError(path)

        self.path: Path = path
        self.storage_dir: Path = Path(self.path, "storage")
        self.temp_dir: Path = Path(self.path, "temp")
        self.temp_out_dir: Path = Path(self.temp_dir, "out")
        self._cached_files: _StoredFilesDict = {}  # map hashes to files
        self._cached_files_total_size: int = 0  # in bytes
        self.thumb_memory_cache = BytesLimitLRU(storage_settings.thumb_cache_max_mb * 1024 * 1024)
        self.distribution_controller: DistributionController = DistributionController()

        create_dir_if_not_exists(self.temp_dir, 0o755)
        create_dir_if_not_exists(self.temp_out_dir, 0o777, explicit_chmod=True)

        self._garbage_collector_task = asyncio.create_task(self._garbage_collect_cron())
        TaskManager.fire_and_forget_task(self._garbage_collector_task)

    @classmethod
    def get_instance(cls) -> "FileStorage":
        if cls._instance is None:
            cls._instance = FileStorage(Path(storage_settings.files_path))
            cls._instance.distribution_controller.start_periodic_reset_task()
        return cls._instance

    def load_file_list(self) -> None:
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
                                storage_logger.info("Skip logging the other files that were found")
                            else:
                                storage_logger.info(f"Found video {f}")
        except Exception as e:
            storage_logger.exception(f"{str(e)} in load_file_list")
        storage_logger.info(f"Found {len(self._cached_files)} videos in storage")

    def _add_video_to_cache(self, file_hash: str, file_extension: str, file_path: Path) -> StoredHashedVideoFile:
        file = StoredHashedVideoFile(file_hash, file_extension)
        try:
            stat = file_path.stat()
        except FileNotFoundError:
            storage_logger.exception("FileNotFoundError in _add_video_to_cache")
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

    def all_files(self) -> _StoredFilesDict:
        return deepcopy(self._cached_files)

    async def filtered_files(self, orphaned: bool = None, extensions: Iterable[str] = VALID_EXTENSIONS,
                             types: Iterable[str] = FileType.values()) -> _StoredFilesDict:
        """
        Returns a dictionary of stored files (keys being the files' hashes) that match the filter criteria.

        All parameters act as filters;
        if None is passed as an argument, no filtering is applied with respect to that parameter.

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
        files = self.all_files()
        hashes_orphaned = {} if orphaned is None else await self._file_hashes_orphaned_dict(files)

        extensions = set(extensions)
        invalid_extensions = extensions.difference(VALID_EXTENSIONS)
        if invalid_extensions:
            raise ValueError(f"Invalid file extension(s): {invalid_extensions}")

        types = set(types)
        invalid_types = types.difference(FileType.values())
        if invalid_types:
            raise ValueError(f"Invalid file type(s): {invalid_types}")

        filtered = {}
        for file_hash, file in files.items():
            if file.file_extension not in extensions:
                continue
            if hashes_orphaned and hashes_orphaned[file_hash] != orphaned:
                continue
            # TODO: Filter by file type; this is a placeholder:
            assert types is not None
            filtered[file_hash] = file
        return filtered

    async def _file_hashes_orphaned_dict(self, hashed_files_dict: _StoredFilesDict = None) -> Dict[str, bool]:
        """
        Checks the orphan status for stored files.
        A file is defined to be orphaned, iff not a single LMS knows about its existence.

        Args:
            hashed_files_dict (optional):
                If passed a dictionary of stored files (with keys being their hashes), only they are checked;
                by default all stored files (at the moment of the method call) are checked.

        Returns:
            Dictionary with keys being hashes of the files that were checked, and values being booleans
            indicating whether the file with the corresponding hash is an orphan.
        """
        hash_orphaned_dict: Dict[str, bool] = {}  # Output
        # Copy currently cached files, if none were passed, and disregard storage changes from here on
        if hashed_files_dict is None:
            hashed_files_dict = self.all_files()
        storage_logger.info(f"Checking {len(hashed_files_dict)} files for their orphan status...")
        # Gather calls to lms_has_file(...) coroutines for each file; the awaited result is a list of booleans.
        existing: List[bool] = await gather_in_batches(20, *(lms_has_file(f) for f in hashed_files_dict.values()))
        # Both the `files` dictionary (since Python 3.6) and the `asyncio.gather` function preserve order,
        # therefore each hash's index in `files.keys()` can be used to get the corresponding `existing` value.
        for idx, key in enumerate(hashed_files_dict.keys()):
            hash_orphaned_dict[key] = not existing[idx]
        return hash_orphaned_dict

    async def get_file(self, file_hash: str, file_extension: str) -> StoredHashedVideoFile:
        """Get video file in storage and check that it really exists."""
        file = self._cached_files.get(file_hash)
        if file and file.file_extension == file_extension:
            return file

        raise FileNotFoundError()

    async def generate_thumbs(self, file: HashedVideoFile, video: VideoInfo) -> int:
        """Generates thumbnail suggestions."""
        video_length = video.get_length()
        thumb_height = storage_settings.thumb_height
        thumb_count = storage_settings.thumb_suggestion_count
        video_check_user = storage_settings.check_user
        # Generate thumbnails concurrently
        tasks = []
        for thumb_nr in range(thumb_count):
            thumb_path = self.get_thumb_path_in_temp(file, thumb_nr)
            offset = int(video_length / thumb_count * (thumb_nr + 0.5))
            temp_out_file = None
            if video_check_user is not None:
                temp_out_file = Path(self.temp_out_dir, file.hash + "_" + str(thumb_nr) + JPG_EXT)
            tasks.append(Video(video_config=VideoConfig(storage_settings)).save_thumbnail(
                video.video_file, thumb_path, offset, thumb_height, temp_output_file=temp_out_file))
        await asyncio.gather(*tasks)
        return thumb_count

    @classmethod
    def get_hash_gen(cls) -> SHA256Type:
        """Get hashing method that is used for all files in the video."""
        return hashlib.sha256()

    def create_temp_file(self) -> 'TempFile':
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
        storage_logger.info("Added file with hash %s permanently to storage.", file.hash)
        self._add_video_to_cache(file.hash, file.file_extension, new_file_path)

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
        storage_logger.info(f"Added {thumb_count} thumbnails for file with hash {file.hash} permanently to storage.")

    async def remove(self, file: StoredHashedVideoFile) -> None:
        file_path = self.get_path(file)

        # Run in another thread as there is blocking io.
        if not await asyncio.get_event_loop().run_in_executor(None, self._delete_file, file_path):
            raise FileNotFoundError()

        # Remove file from cached files and delete all copies on distributor nodes.
        self._cached_files.pop(file.hash)
        self._cached_files_total_size -= file.file_size
        self.distribution_controller.remove_video(file)

        storage_logger.info(f"Removed file with hash {file.hash} permanently from storage.")

    async def remove_thumbs(self, file: HashedVideoFile) -> None:
        thumb_nr = 0
        # Remove increasing thumbnail ids until file not found
        while True:
            thumb_path = self.get_thumb_path(file, thumb_nr)
            # Run in another thread as there is blocking io.
            if not await asyncio.get_event_loop().run_in_executor(None, self._delete_file, thumb_path):
                break
            thumb_nr += 1

        storage_logger.info(f"Removed {thumb_nr} thumbnails for file with hash {file.hash} permanently from storage.")

    async def remove_files(self, *hashes: str) -> List[bool]:
        """
        Gathers and awaits calls to the check_lms_and_remove_file method,
        passing one of the files into each call.
        Returns a list of booleans, the value of which signifies whether the file with the corresponding index
        was successfully deleted.
        """
        return await gather_in_batches(20, *(self.check_lms_and_remove_file(self._cached_files[h]) for h in hashes))

    async def check_lms_and_remove_file(self, file: StoredHashedVideoFile, origin: str = None) -> bool:
        """
        If lms_has_file(...) returns False, indicating that the no site (except the origin, if passed) knows the file,
        the file is removed.

        Args:
            file:
                Self-explanatory
            origin (optional):
                Passed to lms_has_file(...)

        Returns:
            True, if removal of the video file (and its thumbnails) was successful; False otherwise.
        """
        try:
            file_is_known = await lms_has_file(file, origin=origin)
        except LMSAPIError:
            storage_logger.info(f"Video delete: Could not check all LMS for file: {file}. Not deleting.")
            return False
        if file_is_known:
            storage_logger.info(f"Video delete: One LMS still has the video. Do not delete.")
            return False
        await self.remove_thumbs(file)
        await self.remove(file)
        storage_logger.info(f"Deleted video {file} permanently")
        return True

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

    async def _garbage_collect_cron(self):
        """Endless loop that cleans up data periodically."""
        while True:
            files_deleted = await asyncio.get_event_loop().run_in_executor(None, self.garbage_collect_temp_dir)
            if files_deleted > 0:
                storage_logger.info(f"Run GC of temp folder: Removed {files_deleted} file(s).")

            await asyncio.sleep(self.GC_ITERATION_SECS)


class TempFile:
    """Used to handle files that are getting uploaded right now or were just uploaded, but not yet added to the
    file storage finally.
    """

    def __init__(self, file: BinaryIO, path: Path, storage: FileStorage):
        self.hash = FileStorage.get_hash_gen()
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

    async def close(self):
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
        return Path(temp_dir, file.hash + file.file_extension)

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
    storage_logger.info(f"Delete video with hash {file_hash}")
    TaskManager.fire_and_forget_task(asyncio.create_task(_video_delete_task(file_hash, file_ext, origin)))


async def _video_delete_task(file_hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    file_storage = FileStorage.get_instance()
    try:
        file = await file_storage.get_file(file_hash, file_ext)
    except FileNotFoundError:
        storage_logger.info(f"Video delete: file not found: {file_hash}{file_ext}.")
        return
    await file_storage.check_lms_and_remove_file(file, origin=origin)


async def lms_has_file(file: StoredHashedVideoFile, origin: str = None) -> bool:
    """
    Checks LMS Sites for the existence of a stored file on one of them.
    Assuming all LMS sites of interest are checked and this returns False, the file is referred to as *orphaned*.
    Re-raises LMSAPIError after logging it.

    Args:
        file:
            Instance of a stored video file (StoredHashedVideoFile class), the existence of which should be checked
        origin (optional):
            If passed a string, the site with a matching base url is excluded from the check, presumably because
            it is the site requesting the deletion and thus still has the file in question.

    Returns:
        True, if the video file in question exists on **at least one** of the LMS sites; False otherwise.
    """
    for site in LMSSitesCollection.get_all().sites:
        if origin and site.base_url.startswith(origin):
            continue
        storage_logger.debug(f"Checking LMS {site.base_url} for file {file}.")
        try:
            exists = await site.video_exists(file.hash, file.file_extension)
        except LMSAPIError:
            storage_logger.warning(f"LMSAPIError occurred on {site.base_url}.")
            raise
        else:
            if exists:  # Else, continue looping through sites
                storage_logger.debug(f"The site {site.base_url} has video {file}")
                return True
    return False
