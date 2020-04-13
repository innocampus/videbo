import asyncio
import hashlib
import os
import pathlib
import tempfile
import time
import shutil
from _sha256 import SHA256Type
from typing import Optional, Tuple, Dict, BinaryIO

from livestreaming.misc import TaskManager
from . import storage_logger
from . import storage_settings
from .distribution import DistributionController, FileNodes
from .video import VideoInfo
from .exceptions import *

FILE_EXT_WHITELIST = ['.mp4', '.webm']
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

    def __init__(self, path: str):
        if not os.path.isdir(path):
            storage_logger.fatal(f"videos dir {path} does not exist")
            raise NotADirectoryError()

        self.path: str = path
        self.tempdir: str = path + '/temp'
        self.temp_out_dir: str = path + '/temp/out'
        self._cached_files: Dict[str, StoredHashedVideoFile] = {}  # map hashes to files
        self.distribution_controller : DistributionController = DistributionController()

        if not os.path.isdir(self.tempdir):
            os.mkdir(self.tempdir, mode=0o755)
            if not os.path.isdir(self.tempdir):
                raise CouldNotCreateTempDir()
            storage_logger.info('Created temp dir in videos dir')

        if not os.path.isdir(self.temp_out_dir):
            os.mkdir(self.temp_out_dir, mode=0o777)
            os.chmod(self.temp_out_dir, 0o777)  # Ensure public
            if not os.path.isdir(self.temp_out_dir):
                raise CouldNotCreateTempOutDir()
            storage_logger.info('Created out dir in temp dir')

        self._garbage_collector_task = asyncio.create_task(self._garbage_collect_cron())
        TaskManager.fire_and_forget_task(self._garbage_collector_task)

    @classmethod
    def get_instance(cls) -> "FileStorage":
        if cls._instance is None:
            cls._instance = FileStorage(str(storage_settings.videos_path))
            cls._instance.distribution_controller.start_periodic_reset_task()
        return cls._instance

    async def get_file(self, file_hash: str, file_extension: str) -> StoredHashedVideoFile:
        """Get video file in storage and check that it really exists."""
        file = self._cached_files.get(file_hash)
        if file:
            if file.file_extension != file_extension:
                raise FileDoesNotExistError()
            return file

        file = StoredHashedVideoFile(file_hash, file_extension)
        path = pathlib.Path(self.get_path(file)[0])
        if not (await asyncio.get_event_loop().run_in_executor(None, path.is_file)):
            storage_logger.warning(f"file does not exist: {path}")
            raise FileDoesNotExistError()

        stat = await asyncio.get_event_loop().run_in_executor(None, path.stat)
        file.file_size = stat.st_size

        # Add to cached files, but check if another coroutine added the file meanwhile.
        check_file = self._cached_files.get(file_hash)
        if check_file:
            return check_file

        self._cached_files[file.hash] = file
        self.distribution_controller.add_video(file)
        return file

    async def generate_thumbs(self, file: HashedVideoFile, video: VideoInfo) -> int:
        """Generates thumbnail suggestions."""
        video_length = video.get_length()
        thumb_height = storage_settings.thumb_height
        thumb_count = storage_settings.thumb_suggestion_count

        ffmpeg_binary = storage_settings.binary_ffmpeg
        video_check_user = storage_settings.check_user

        # Generate thumbnails concurrently
        tasks = []
        for thumb_nr in range(thumb_count):
            thumb_path = self.get_thumb_path_in_temp(file, thumb_nr)
            offset = int(video_length / thumb_count * (thumb_nr + 0.5))
            temp_out_file = None
            if video_check_user is not None:
                temp_out_file = self.temp_out_dir + "/" + file.hash + "_" + str(thumb_nr) + THUMB_EXT
            tasks.append(self.save_thumbnail(video.video_file, thumb_path, offset, thumb_height, binary=ffmpeg_binary,
                                             user=video_check_user, temp_output_file=temp_out_file))
        await asyncio.gather(*tasks)

        return thumb_count

    @classmethod
    def get_hash_gen(cls) -> SHA256Type:
        """Get hashing method that is used for all files in the video."""
        return hashlib.sha256()

    def create_temp_file(self) -> 'TempFile':
        """Create a space where we can write data to."""
        fd, path = tempfile.mkstemp(prefix='upload_', dir=self.tempdir)  # actually blocking io
        os.chmod(path, 0o644)  # Make readable for check_user
        return TempFile(os.fdopen(fd, mode='wb'), path, self)

    def get_path(self, file: HashedVideoFile) -> Tuple[str, str]:
        """Get path where to find a file with a hash and the directory."""
        file_dir = self.path + "/" + file.hash[0:2]
        file = file_dir + "/" + file.hash + file.file_extension
        return file, file_dir

    def get_path_in_temp(self, file: HashedVideoFile) -> str:
        return TempFile.get_path(self.tempdir, file)

    def get_thumb_path(self, file: HashedVideoFile, thumb_nr: int) -> Tuple[str, str]:
        """Get path where to find a thumbnail with a hash and the directory."""
        thumb_dir = self.path + "/" + file.hash[0:2]
        file = thumb_dir + "/" + file.hash + "_" + str(thumb_nr) + THUMB_EXT
        return file, thumb_dir

    def get_thumb_path_in_temp(self, file: HashedVideoFile, thumb_nr: int) -> str:
        return TempFile.get_thumb_path(self.tempdir, file, thumb_nr)

    @staticmethod
    def delete_file(file_path) -> bool:
        # Check source file really exists.
        if not os.path.isfile(file_path):
            return False

        os.remove(file_path)
        return True

    @staticmethod
    def copy_file(source_file: str, target_file: str) -> None:
        # Check source file really exists.
        if not os.path.isfile(source_file):
            raise FileDoesNotExistError()

        # Copy if destination doesn't exist
        if not os.path.isfile(target_file):
            shutil.copyfile(source_file, target_file)

    @staticmethod
    def move_file(path: str, new_file_path: str, new_dir_path: str) -> None:
        # Check source file really exists.
        if not os.path.isfile(path):
            raise FileDoesNotExistError()

        # Ensure dir exists.
        os.makedirs(new_dir_path, 0o755, True)

        if os.path.isfile(new_file_path):
            # If a file with the hash already exists, we don't need another copy.
            os.remove(path)
        else:
            os.rename(path, new_file_path)
            os.chmod(new_file_path, 0o644)

    async def add_file_from_temp(self, file: HashedVideoFile) -> None:
        """Add a file to the storage that is currently stored in the temp dir."""
        temp_path = self.get_path_in_temp(file)
        new_file_path, new_dir_path = self.get_path(file)

        # Run in another thread as there is blocking io.
        await asyncio.get_event_loop().run_in_executor(None, self.move_file, temp_path, new_file_path, new_dir_path)
        storage_logger.info("Added file with hash %s permanently to storage.", file.hash)

    async def add_thumbs_from_temp(self, file: HashedVideoFile, thumb_count: int) -> None:
        """Add thumbnails to the video that are currently stored in the temp dir."""
        tasks = []
        for thumb_nr in range(thumb_count):
            old_thumb_path = self.get_thumb_path_in_temp(file, thumb_nr)
            new_thumb_file, new_thumb_dir = self.get_thumb_path(file, thumb_nr)

            # Run in another thread as there is blocking io.
            tasks.append(asyncio.get_event_loop().run_in_executor(None, self.move_file, old_thumb_path, new_thumb_file,
                                                                  new_thumb_dir))
        await asyncio.gather(*tasks)
        storage_logger.info(f"Added {thumb_count} thumbnails for file with hash {file.hash} permanently to storage.")

    async def remove(self, file: StoredHashedVideoFile) -> None:
        file_path = self.get_path(file)[0]

        # Run in another thread as there is blocking io.
        if not await asyncio.get_event_loop().run_in_executor(None, self.delete_file, file_path):
            raise FileDoesNotExistError()

        # Remove file from cached files and delete all copies on distributor nodes.
        self._cached_files.pop(file.hash)
        self.distribution_controller.remove_video(file)

        storage_logger.info(f"Removed file with hash {file.hash} permanently from storage.")

    async def remove_thumbs(self, file: HashedVideoFile) -> None:
        thumb_nr = 0
        # Remove increasing thumbnail ids until file not found
        while True:
            thumb_path = self.get_thumb_path(file, thumb_nr)[0]
            # Run in another thread as there is blocking io.
            if not await asyncio.get_event_loop().run_in_executor(None, self.delete_file, thumb_path):
                break
            thumb_nr += 1

        storage_logger.info(f"Removed {thumb_nr} thumbnails for file with hash {file.hash} permanently from storage.")

    def garbage_collect_temp_dir(self) -> int:
        """Delete files older than GC_TEMP_FILES_SECS."""
        count = 0
        old = time.time() - self.GC_TEMP_FILES_SECS
        for filename in os.listdir(self.tempdir):
            path = self.tempdir + "/" + filename
            if os.path.isfile(path) and os.path.getmtime(path) < old:
                # File is old and most likely not needed anymore.
                os.remove(path)
                count += 1

        return count

    async def _garbage_collect_cron(self):
        """Endless loop that cleans up data periodically."""
        while True:
            files_deleted = await asyncio.get_event_loop().run_in_executor(None, self.garbage_collect_temp_dir)
            if files_deleted > 0:
                storage_logger.info(f"Run GC of temp folder: Removed {files_deleted} file(s).")

            await asyncio.sleep(self.GC_ITERATION_SECS)

    async def save_thumbnail(self, video_in: str, thumbnail_out: str, offset: int, height: int, binary: str = "ffmpeg",
                             user: str = None, temp_output_file: str = None) -> None:
        """Call ffmpeg and save scaled frame at specified offset."""
        output_file = thumbnail_out
        if temp_output_file is not None:
            output_file = temp_output_file

        args = ["-ss", str(offset), "-i", video_in, "-vframes", "1", "-an", "-vf",
                "scale=-1:{0}".format(height), "-y", output_file]
        # Run ffmpeg
        if user:
            args = ["-u", user, binary] + args
            binary = "sudo"
        proc = await asyncio.create_subprocess_exec(binary, *args, stdout=asyncio.subprocess.DEVNULL,
                                                    stderr=asyncio.subprocess.DEVNULL)
        try:
            await asyncio.wait_for(proc.wait(), 10)
            if proc.returncode != 0:
                raise FFMpegError(False)

            if output_file != thumbnail_out:
                # Copy temp file to target
                await asyncio.get_event_loop().run_in_executor(None, self.copy_file, output_file, thumbnail_out)

        except asyncio.TimeoutError:
            proc.kill()
            raise FFMpegError(True)

        finally:
            if output_file != thumbnail_out:
                # Delete temp file
                await asyncio.get_event_loop().run_in_executor(None, self.delete_file, output_file)


class TempFile:
    """Used to handle files that are getting uploaded right now or were just uploaded, but not yet added to the
    file storage finally.
    """

    def __init__(self, file: BinaryIO, path: str, storage: FileStorage):
        self.hash = FileStorage.get_hash_gen()
        self.file = file
        self.path = path
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
            new_path = self.get_path(self.storage.tempdir, file)
            if os.path.isfile(new_path):
                # If a file with the hash already exists, we don't need another copy.
                os.remove(self.path)
            else:
                os.rename(self.path, new_path)

        await asyncio.get_event_loop().run_in_executor(None, move)
        return file

    @classmethod
    def get_path(cls, tempdir: str, file: HashedVideoFile) -> str:
        return tempdir + "/" + file.hash + file.file_extension

    @classmethod
    def get_thumb_path(cls, tempdir: str, file: HashedVideoFile, thumb_nr: int) -> str:
        return tempdir + "/" + file.hash + "_" + str(thumb_nr) + THUMB_EXT

    async def delete(self):
        def delete_file():
            self.file.close()
            if os.path.isfile(self.path):
                os.remove(self.path)

        await asyncio.get_event_loop().run_in_executor(None, delete_file)


def is_allowed_file_ending(filename: Optional[str]) -> bool:
    """Simple check that the file ending is on the whitelist."""
    if filename is None:
        return True
    return filename.lower().endswith(tuple(FILE_EXT_WHITELIST))
