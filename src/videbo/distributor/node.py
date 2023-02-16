from __future__ import annotations
from asyncio.tasks import Task, sleep as async_sleep
from logging import getLogger
from typing import Optional, Iterable

from videbo import settings
from videbo.client import Client
from videbo.exceptions import HTTPClientError, NoRunningTask
from videbo.misc import MEGA
from videbo.misc.task_manager import TaskManager
from videbo.storage.exceptions import (
    DistNodeAlreadyDisabled,
    DistNodeAlreadyEnabled,
    DistStatusUnknown,
)
from videbo.storage.stored_file import StoredVideoFile
from videbo.types import FileID
from .api.models import (
    DistributorStatus,
    DistributorFileList,
    DistributorCopyFile,
    DistributorDeleteFilesResponse,
    DistributorDeleteFiles,
)
from .scheduler import DownloadScheduler


log = getLogger(__name__)


class DistributorNode:
    def __init__(self, base_url: str, http_client: Optional[Client] = None) -> None:
        self.base_url: str = base_url
        self.http_client: Client = Client() if http_client is None else http_client
        self.status: Optional[DistributorStatus] = None
        self.stored_videos: set[StoredVideoFile] = set()
        self.loading: set[StoredVideoFile] = set()  # Node is currently downloading these files.
        self.awaiting_download = DownloadScheduler()  # Files waiting to be downloaded
        self.watcher_task: Optional[Task[None]] = None
        self._good: bool = False  # node is reachable
        self._enabled: bool = True

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DistributorNode):
            return NotImplemented
        return self.base_url == other.base_url

    def __lt__(self, other: DistributorNode) -> bool:
        return self.tx_load < other.tx_load

    @property
    def is_enabled(self) -> bool:
        return self._enabled

    @property
    def is_good(self) -> bool:
        return self._good

    @property
    def tx_load(self) -> float:
        if self.status is None:
            raise DistStatusUnknown
        return self.status.tx_current_rate / self.status.tx_max_rate

    @property
    def free_space(self) -> float:
        if self.status is None:
            raise DistStatusUnknown
        return self.status.free_space

    @free_space.setter
    def free_space(self, megabytes: float) -> None:
        if self.status is None:
            raise DistStatusUnknown
        self.status.free_space = megabytes

    @property
    def total_space(self) -> float:
        if self.status is None:
            raise DistStatusUnknown
        return self.status.free_space + self.status.files_total_size

    @property
    def free_space_ratio(self) -> float:
        if self.status is None:
            raise DistStatusUnknown
        return self.status.free_space / self.total_space

    @property
    def can_serve(self) -> bool:
        return self.is_enabled and self.tx_load < 0.95 and self.is_good

    def can_host_additional(self, min_space_mb: float) -> bool:
        try:
            return self.can_serve and self.free_space > min_space_mb
        except DistStatusUnknown as e:
            log.error(
                "%s while checking viability of distributor %s",
                e.__class__.__name__,
                self.base_url,
            )
            return False

    def can_provide_copy(self, file: StoredVideoFile) -> bool:
        return self.is_enabled and file not in self.loading and self.is_good

    async def watcher(self) -> None:
        url = self.base_url + '/api/distributor/status'
        print_exception = True
        code: int
        ret: DistributorStatus
        while True:
            try:
                code, ret = await self.http_client.request(
                    "GET",
                    url,
                    self.http_client.get_jwt_node(),
                    return_model=DistributorStatus,
                    log_connection_error=print_exception,
                )
            except HTTPClientError:
                print_exception = False
                if self.is_good:
                    log.error(f"<Distribution watcher {self.base_url}> http error")
                    await self.set_node_state(False)
            else:
                print_exception = True
                if code == 200:
                    self.status = ret
                    if not self.is_good:
                        log.info(f"<Distribution watcher {self.base_url}> connected. "
                                 f"Free space currently: {self.free_space} MB")
                        await self.set_node_state(True)
                elif self.is_good:
                    log.error(f"<Distribution watcher {self.base_url}> http status {code}")
                    await self.set_node_state(False)
            await async_sleep(5)

    def start_watching(self) -> None:
        self.watcher_task = TaskManager.fire_and_forget(
            self.watcher(),
            name="distributor_node_watcher",
        )

    def stop_watching(self) -> None:
        if self.watcher_task is None:
            raise NoRunningTask
        self.watcher_task.cancel()
        self.watcher_task = None

    async def set_node_state(self, new_is_in_good_state: bool) -> None:
        # File list is loaded before node status switches to good.
        if self.is_good and not new_is_in_good_state:
            await self.unlink_node(False)
        elif not self.is_good and new_is_in_good_state:
            await self._load_file_list()
        self._good = new_is_in_good_state

    async def _load_file_list(self) -> None:
        """Fetch a list of all files that the node currently has."""
        from videbo.storage.util import FileStorage
        storage = FileStorage.get_instance()
        remove_unknown_files: list[FileID] = []
        url = self.base_url + '/api/distributor/files'
        ret: DistributorFileList
        try:
            code, ret = await self.http_client.request(
                "GET",
                url,
                self.http_client.get_jwt_node(),
                return_model=DistributorFileList,
            )
        except HTTPClientError:
            log.exception(f"<Distribution watcher {self.base_url}> http error")
        else:
            if code != 200:
                log.error(f"<Distribution watcher {self.base_url}> http status {code}")
                return
            for file_hash, file_ext in ret.files:
                try:
                    file = storage.get_file(file_hash, file_ext)
                except FileNotFoundError:
                    log.info(f"Remove `{file_hash}{file_ext}` on {self.base_url} since file does not exist on storage.")
                    remove_unknown_files.append((file_hash, file_ext))
                else:
                    self.stored_videos.add(file)
                    file.nodes.append(self)
            log.info(f"Found {len(self.stored_videos)} files on {self.base_url}")
            if remove_unknown_files:
                await self._remove_files(remove_unknown_files)

    def put_video(self, file: StoredVideoFile, from_node: Optional[DistributorNode] = None) -> None:
        """Copy a video from one node to another. If `from_node` is None, copy from the storage node."""
        if file in self.loading or file in self.awaiting_download:
            return
        from_url = from_node.base_url if from_node else settings.public_base_url
        if len(self.loading) < settings.distribution.max_parallel_copying_tasks:
            TaskManager.fire_and_forget(self._copy_file_task(file, from_url))
        else:
            self.awaiting_download.schedule(file, from_url)

    async def _copy_file_task(self, file: StoredVideoFile, from_url: str) -> None:
        file.nodes.append(self)
        file.copying = True
        self.loading.add(file)
        url = f'{self.base_url}/api/distributor/copy/{file}'
        data = DistributorCopyFile(from_base_url=from_url, file_size=file.size)
        log.info(f"Asking distributor to copy `{file}` from {from_url} to {self.base_url}")
        try:
            code, ret = await self.http_client.request(
                "POST",
                url,
                self.http_client.get_jwt_node(),
                data=data,
                timeout=30. * 60,
            )
        except Exception as e:
            file.nodes.remove(self)  # This node cannot serve the file
            log.exception(f"Error when copying `{file}` from {from_url} to {self.base_url}")
            raise e
        else:
            if code == 200:
                self.stored_videos.add(file)  # Successfully copied; this node can now serve the file
                log.info(f"Copied `{file}` from {from_url} to {self.base_url}")
            else:
                file.nodes.remove(self)  # This node cannot serve the file
                log.error(f"Error copying `{file}` from {from_url} to {self.base_url}, http status {code}")
        finally:
            # Regardless of copying success or failure, always remove the file from `.loading`
            # and set the file's FileNodes `.copying` attribute to `False`.
            self.loading.discard(file)
            file.copying = False
            if len(self.loading) >= settings.distribution.max_parallel_copying_tasks:
                return
            # Check if other files are scheduled for download and if they are, fire off the next copying task.
            try:
                next_file, next_from_url = self.awaiting_download.next()
            except DownloadScheduler.NothingScheduled:
                return
            TaskManager.fire_and_forget(self._copy_file_task(next_file, next_from_url))

    async def _remove_files(self, rem_files: list[FileID], safe: bool = True) -> DistributorDeleteFilesResponse:
        url = f'{self.base_url}/api/distributor/delete'
        data = DistributorDeleteFiles(files=rem_files, safe=safe)
        number = len(rem_files)
        ret: DistributorDeleteFilesResponse
        log.info(f"Asking distributor to remove {number} file(s) from {self.base_url}")
        try:
            code, ret = await self.http_client.request(
                "POST",
                url,
                self.http_client.get_jwt_node(),
                data=data,
                return_model=DistributorDeleteFilesResponse,
                timeout=60.,
            )
        except Exception as e:
            log.exception(f"{e} removing {number} file(s) from {self.base_url}")
            raise e
        if code == 200:
            log.info(f"Removed {number} file(s) from {self.base_url}")
            self.free_space = ret.free_space
        else:
            log.error(f"Error removing {number} file(s) from {self.base_url}, http status {code}")
        return ret

    async def remove_videos(self, files: Iterable[StoredVideoFile], safe: bool = True) -> None:
        hashes_extensions, to_discard = [], {}
        for file in files:
            hashes_extensions.append((file.hash, file.ext))
            to_discard[f'{file.hash}{file.ext}'] = file
        response_data = await self._remove_files(hashes_extensions, safe=safe)  # calls the distributor's API
        # Only discard files that were actually deleted:
        not_deleted_size = 0
        for file_hash, file_ext in response_data.files_skipped:
            file = to_discard.pop(f'{file_hash}{file_ext}')
            not_deleted_size += file.size
        if not_deleted_size:
            log.warning(f"{len(response_data.files_skipped)} file(s) taking up {not_deleted_size} MB "
                        f"could not be deleted from {self.base_url}")
        for file in to_discard.values():
            self.stored_videos.discard(file)
            file.nodes.remove(self)
        log.info(f"Removed {len(to_discard)} file(s) (having now {response_data.free_space} MB free space) on {self.base_url}")

    async def unlink_node(self, stop_watching: bool = True) -> None:
        """Do not remove videos on this node, but remove all local references to this node."""
        for file in self.stored_videos:
            file.nodes.remove(self)
        self.stored_videos.clear()
        if stop_watching:
            self.stop_watching()

    async def free_up_space(self) -> None:
        """Attempts to remove less popular videos to free-up disk space on the distributor node."""
        if not self.stored_videos or not self._enabled:
            return
        if not self.is_good:
            log.info(f"Distributor {self.base_url} is not in a good state. Skip freeing space.")
            return
        assert 0 <= settings.distribution.free_space_target_ratio <= 1
        if self.free_space_ratio >= settings.distribution.free_space_target_ratio:
            log.debug(f"{int(self.free_space)} MB ({round(self.free_space_ratio * 100, 1)} %) "
                      f"of free space available on {self.base_url}")
            return
        target_gain = (settings.distribution.free_space_target_ratio * self.total_space - self.free_space) * MEGA
        to_remove, space_gain = [], 0
        sorted_videos = sorted(self.stored_videos, reverse=True)
        while space_gain < target_gain:
            video = sorted_videos.pop()
            to_remove.append(video)
            space_gain += video.size
        log.info(f"Trying to purge {len(to_remove)} less popular video(s) "
                 f"to free up {space_gain / MEGA} MB of space at {self.base_url}")
        await self.remove_videos(to_remove)

    def disable(self, stop_watching: bool = True) -> None:
        if not self._enabled:
            log.warning(f"Already disabled distributor node `{self.base_url}`")
            raise DistNodeAlreadyDisabled(self.base_url)
        self._enabled = False
        if stop_watching:
            self.stop_watching()

    def enable(self) -> None:
        if self._enabled:
            log.warning(f"Already enabled distributor node `{self.base_url}`")
            raise DistNodeAlreadyEnabled(self.base_url)
        self._enabled = True
        if self.watcher_task is None:
            self.start_watching()
