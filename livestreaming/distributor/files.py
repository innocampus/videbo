import os
from asyncio import get_running_loop, Event, wait_for, TimeoutError
from pathlib import Path
from typing import Optional, Dict, Union
from livestreaming.auth import internal_jwt_encode
from livestreaming.misc import get_free_disk_space
from livestreaming.web import HTTPClient
from livestreaming.storage.util import HashedVideoFile
from livestreaming.storage.api.models import RequestFileJWTData, FileType
from . import logger, distributor_settings


class DistributorHashedVideoFile(HashedVideoFile):
    __slots__ = "event"

    def __init__(self, file_hash: str, file_extension: str):
        super().__init__(file_hash, file_extension)
        # When event is set, we know that the file was not yet downloaded completely.
        self.event: Optional[Event] = None


class DistributorFileController:
    MAX_WAITING_CLIENTS = 30

    def __init__(self):
        # file hash -> Event if the node is still downloading the file right now (event is fired when load completed)
        self.files: Dict[str, DistributorHashedVideoFile] = {}
        self.base_path: Optional[Path] = None
        self.waiting: int = 0  # number of clients clients waiting for a file being downloaded

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
                self.files[file_hash] = DistributorHashedVideoFile(file_hash, "." + file_split[1])

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
            if isinstance(found, Event):
                if self.waiting >= self.MAX_WAITING_CLIENTS:
                    raise TooManyWaitingClients()

                try:
                    self.waiting += 1
                    await wait_for(found.wait(), wait)
                finally:
                    self.waiting -= 1
            return True

        return False

    async def get_free_space(self) -> int:
        """Returns free space in MB excluding the space that should be empty."""
        free = await get_free_disk_space(str(self.base_path))
        return max(free - distributor_settings.leave_free_space_mb, 0)

    async def copy_file(self, file: HashedVideoFile, from_url: str, expected_file_size: int) -> None:
        if file.hash in self.files:
            # File is already there or it is downloaded right now.
            return

        logger.info(f"Start copying file {file} from {from_url}")
        event = Event()
        new_file = DistributorHashedVideoFile(file.hash, file.file_extension)
        new_file.event = event
        self.files[file.hash] = new_file
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

            async with HTTPClient.session.request("GET", from_url + "/file", headers=headers) as response:
                if response.status != 200:
                    logger.error(f"Error when copying file {file} from {from_url}: got http status {response.status}")
                    raise CopyFileError()

                # Check if we have enough space for this file.
                file_size = int(response.headers.getone("Content-Length", 0)) / 1024 / 1024
                if file_size > free_space:
                    logger.error(f"Error when copying file {file} from {from_url}: Not enough space, "
                                 f"free space {free_space} MB, file is {file_size} MB")
                    raise CopyFileError()

                # Load file
                loaded_bytes = 0
                while True:
                    data = await response.content.read(1024 * 1024)
                    if len(data) == 0:
                        break
                    loaded_bytes += len(data)
                    await get_running_loop().run_in_executor(None, file_obj.write, data)
                if loaded_bytes != expected_file_size:
                    logger.error(f"Error when copying file {file} from {from_url}: Loaded {loaded_bytes} bytes,"
                                 f"but expected {expected_file_size} bytes.")
                    raise CopyFileError()
                logger.info(f"Copied file {file} from {from_url}")
        except:
            # Set event to wake up all waiting tasks even though we don't have the file.
            event.set()
            self.files.pop(file.hash)
            logger.exception(f"Error when copying file {file} from {from_url}")
            raise CopyFileError()
        finally:
            if file_obj:
                await get_running_loop().run_in_executor(None, file_obj.close)

        # move to final location (without .tmp suffix)
        final_path = self.get_path(file)
        await get_running_loop().run_in_executor(None, temp_path.rename, final_path)
        event.set()
        self.files[file.hash].event = None

    async def delete_file(self, file: HashedVideoFile) -> None:
        try:
            self.files.pop(file.hash)
        except KeyError:
            return

        path = self.get_path(file)
        await get_running_loop().run_in_executor(None, path.unlink)


file_controller = DistributorFileController()


class CopyFileError(Exception):
    pass


class TooManyWaitingClients(Exception):
    pass
