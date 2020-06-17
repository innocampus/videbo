import asyncio
import hashlib
import os
import tempfile
import time
from _sha256 import SHA256Type
from pathlib import Path
from typing import Optional, Tuple, Dict, BinaryIO

from videbo.misc import TaskManager
from videbo.lms_api import LMSSitesCollection, LMSAPIError
from videbo.video import VideoInfo
from videbo.video import Video
from videbo.video import VideoConfig

from . import storage_logger
from . import storage_settings
from .distribution import DistributionController, FileNodes
from .exceptions import *

FILE_EXT_WHITELIST = ('.mp4', '.webm')
THUMB_EXT = '.jpg'


class HashedVideoFile:
    __slots__ = "hash", "file_extension"

    def __init__(self, file_hash: str, file_extension: str):
        self.hash = file_hash
        self.file_extension = file_extension

        # Extension has to start with a dot.
        if file_extension[0] != ".":
            raise HashedFileInvalidExtensionError()

    def __str__(self):
        return self.hash + self.file_extension


class StoredHashedVideoFile(HashedVideoFile):
    __slots__ = "file_size", "views", "nodes"

    def __init__(self, file_hash: str, file_extension: str):
        super().__init__(file_hash, file_extension)
        self.file_size: int = -1  # in bytes
        self.views: int = 0
        self.nodes: FileNodes = FileNodes()

    def __lt__(self, other: "StoredHashedVideoFile"):
        """Compare videos by their view counters."""
        return self.views < other.views


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
        self._cached_files: Dict[str, StoredHashedVideoFile] = {}  # map hashes to files
        self._cached_files_total_size: int = 0  # in bytes
        self.distribution_controller: DistributionController = DistributionController()

        if not self.temp_dir.is_dir():
            self.temp_dir.mkdir(mode=0o755)
            if not self.temp_dir.is_dir():
                raise CouldNotCreateTempDir()
            storage_logger.info('Created temp dir in videos dir')

        if not self.temp_out_dir.is_dir():
            self.temp_out_dir.mkdir(mode=0o777)
            self.temp_out_dir.chmod(0o777)  # Ensure public
            if not self.temp_out_dir.is_dir():
                raise CouldNotCreateTempOutDir()
            storage_logger.info('Created out dir in temp dir')

        self._garbage_collector_task = asyncio.create_task(self._garbage_collect_cron())
        TaskManager.fire_and_forget_task(self._garbage_collector_task)

    @classmethod
    def get_instance(cls) -> "FileStorage":
        if cls._instance is None:
            cls._instance = FileStorage(Path(storage_settings.videos_path))
            cls._instance.distribution_controller.start_periodic_reset_task()
        return cls._instance

    def load_file_list(self):
        """Load all video files in memory"""
        try:
            for obj in self.storage_dir.glob('**/*'):
                if obj.is_file():
                    file_split = obj.name.split('.')
                    if len(file_split) == 2:
                        file_hash = file_split[0]
                        file_ext = "." + file_split[1]
                        if file_ext in FILE_EXT_WHITELIST:
                            self._add_video_to_cache(file_hash, file_ext, obj)
                            if len(self._cached_files) < 20:
                                storage_logger.info(f"Found video {file_hash}{file_ext}")

            if len(self._cached_files) >= 20:
                storage_logger.info("Skip logging the other files that were found")
            storage_logger.info(f"Found {len(self._cached_files)} videos in storage")
        except:
            storage_logger.exception("Error in load_file_list")

    def _add_video_to_cache(self, file_hash: str, file_extension: str, file_path: Path):
        file = StoredHashedVideoFile(file_hash, file_extension)
        try:
            stat = file_path.stat()
            file.file_size = stat.st_size
            self._cached_files[file.hash] = file
            self._cached_files_total_size += file.file_size
            self.distribution_controller.add_video(file)
        except FileNotFoundError:
            storage_logger.exception("FileNotFoundError in _add_video_to_cache")

    def get_files_total_size_mb(self) -> int:
        return int(self._cached_files_total_size / 1024 / 1024)

    def get_files_count(self) -> int:
        return len(self._cached_files)

    async def get_file(self, file_hash: str, file_extension: str) -> StoredHashedVideoFile:
        """Get video file in storage and check that it really exists."""
        file = self._cached_files.get(file_hash)
        if file and file.file_extension == file_extension:
            return file

        raise FileDoesNotExistError()

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
                temp_out_file = Path(self.temp_out_dir, file.hash + "_" + str(thumb_nr) + THUMB_EXT)
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
        file_name = file.hash + file.file_extension
        return Path(self.storage_dir, file.hash[0:2], file_name)

    def get_path_in_temp(self, file: HashedVideoFile) -> Path:
        return TempFile.get_path(self.temp_dir, file)

    def get_thumb_path(self, file: HashedVideoFile, thumb_nr: int) -> Path:
        """Get path where to find a thumbnail with a hash."""
        file_name = file.hash + "_" + str(thumb_nr) + THUMB_EXT
        return Path(self.storage_dir, file.hash[0:2], file_name)

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
            raise FileDoesNotExistError()

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
            raise FileDoesNotExistError()

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
        def update_hash_write_file():
            self.hash.update(data)
            self.file.write(data)
            self.is_writing = False

        await asyncio.get_event_loop().run_in_executor(None, update_hash_write_file)

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
        def move():
            new_path = self.get_path(self.storage.temp_dir, file)
            if new_path.is_file():
                # If a file with the hash already exists, we don't need another copy.
                self.path.unlink()
            else:
                self.path.rename(new_path)

        await asyncio.get_event_loop().run_in_executor(None, move)
        return file

    @classmethod
    def get_path(cls, temp_dir: Path, file: HashedVideoFile) -> Path:
        return Path(temp_dir, file.hash + file.file_extension)

    @classmethod
    def get_thumb_path(cls, temp_dir: Path, file: HashedVideoFile, thumb_nr: int) -> Path:
        file_name = file.hash + "_" + str(thumb_nr) + THUMB_EXT
        return Path(temp_dir, file_name)

    async def delete(self):
        def delete_file():
            self.file.close()
            if self.path.is_file():
                self.path.unlink()

        await asyncio.get_event_loop().run_in_executor(None, delete_file)


def is_allowed_file_ending(filename: Optional[str]) -> bool:
    """Simple check that the file ending is on the whitelist."""
    if filename is None:
        return True
    return filename.lower().endswith(FILE_EXT_WHITELIST)


def schedule_video_delete(hash: str, file_ext: str, origin: Optional[str] = None) -> None:
    async def task():
        try:
            file_storage = FileStorage.get_instance()
            file = await file_storage.get_file(hash, file_ext)

            # Check that all LMS do not have the file in their db.
            # Skip origin site as this is the site that is requesting the delete and it still has the file
            # at this moment.
            for site in LMSSitesCollection.get_all().sites:
                if origin is None or not site.base_url.startswith(origin):
                    try:
                        exists = await site.video_exists(hash, file_ext)
                        storage_logger.info(f"Video delete: Site {site.base_url} has video {exists}")
                        if exists:
                            # One site still has the file. Do not delete the file.
                            return
                    except LMSAPIError:
                        # Just in case. When one site cannot be reached, do not delete the file (it may still
                        # have the file).
                        storage_logger.warning(f"Video delete: LMS error occurred for file {hash}{file_ext}. "
                                               f"Do not delete file.")
                        return

            await file_storage.remove_thumbs(file)
            await file_storage.remove(file)
        except FileDoesNotExistError:
            storage_logger.info(f"Video delete: file not found: {hash}{file_ext}")

    storage_logger.info(f"Delete video with hash {hash}")
    TaskManager.fire_and_forget_task(asyncio.create_task(task()))
