from __future__ import annotations
import asyncio
import logging
from collections.abc import Callable, Iterable
from timeit import default_timer as timer
from typing import Optional, TYPE_CHECKING

from videbo import storage_settings as settings
from videbo.exceptions import HTTPResponseError, NoRunningTask
from videbo.misc import MEGA
from videbo.misc.periodic import Periodic
from videbo.misc.task_manager import TaskManager
from videbo.types import FileID
from videbo.web import HTTPClient
from videbo.distributor.api.models import (DistributorCopyFile, DistributorDeleteFiles, DistributorDeleteFilesResponse,
                                           DistributorStatus, DistributorFileList)
from .exceptions import DistStatusUnknown, DistAlreadyEnabled, DistAlreadyDisabled, UnknownDistURL
if TYPE_CHECKING:
    from videbo.storage.util import StoredHashedVideoFile


log = logging.getLogger(__name__)


class DownloadScheduler:
    """
    Helper class to collect files that should be downloaded ASAP.

    Additionally, the `in` operator can be used on an instance to determine if a file is already scheduled,
    and the built-in `len(...)` function can be used get the number of files currently scheduled.
    """
    class NothingScheduled(Exception):
        pass

    def __init__(self) -> None:
        self.files_from_urls: set[tuple[StoredHashedVideoFile, str]] = set()

    def schedule(self, file: StoredHashedVideoFile, from_url: str) -> None:
        self.files_from_urls.add((file, from_url))
        log.info(f"File {file} from {from_url} now scheduled for download")

    def next(self) -> tuple[StoredHashedVideoFile, str]:
        """
        If any files are left to be downloaded, chose the one with the highest number of views.
        Note that the `StoredHashedVideoFile` class compares objects by their `.views` attribute.
        """
        try:
            tup = max(self.files_from_urls, key=lambda x: x[0])
        except ValueError:
            raise self.NothingScheduled
        else:
            self.files_from_urls.discard(tup)
            log.info(f"Next file to download: {tup[0]} from {tup[1]}")
            return tup

    def __contains__(self, item: StoredHashedVideoFile) -> bool:
        for file, _ in self.files_from_urls:
            if file == item:
                return True
        return False

    def __len__(self) -> int:
        return len(self.files_from_urls)


class DistributionNodeInfo:
    def __init__(self, base_url: str):
        self.base_url: str = base_url
        self.status: Optional[DistributorStatus] = None
        self.stored_videos: set[StoredHashedVideoFile] = set()
        self.loading: set[StoredHashedVideoFile] = set()  # Node is currently downloading these files.
        self.awaiting_download = DownloadScheduler()  # Files waiting to be downloaded
        self.watcher_task: Optional[asyncio.Task[None]] = None
        self._good: bool = False  # node is reachable
        self._enabled: bool = True

    def __lt__(self, other: DistributionNodeInfo) -> bool:
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

    async def watcher(self) -> None:
        url = self.base_url + '/api/distributor/status'
        print_exception = True
        code: int
        ret: DistributorStatus
        while True:
            try:
                code, ret = await HTTPClient.internal_request_node('GET', url, expected_return_type=DistributorStatus,
                                                                   print_connection_exception=print_exception)
            except HTTPResponseError:
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
            await asyncio.sleep(5)

    def start_watching(self) -> None:
        self.watcher_task = asyncio.create_task(self.watcher())
        TaskManager.fire_and_forget_task(self.watcher_task)

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
            code, ret = await HTTPClient.internal_request_node('GET', url, expected_return_type=DistributorFileList)
        except HTTPResponseError:
            log.exception(f"<Distribution watcher {self.base_url}> http error")
        else:
            if code != 200:
                log.error(f"<Distribution watcher {self.base_url}> http status {code}")
                return
            for file_hash, file_ext in ret.files:
                try:
                    file = await storage.get_file(file_hash, file_ext)
                except FileNotFoundError:
                    log.info(f"Remove `{file_hash}{file_ext}` on {self.base_url} since file does not exist on storage.")
                    remove_unknown_files.append((file_hash, file_ext))
                else:
                    self.stored_videos.add(file)
                    file.nodes.add_node(self)
            log.info(f"Found {len(self.stored_videos)} files on {self.base_url}")
            if remove_unknown_files:
                await self._remove_files(remove_unknown_files)

    def put_video(self, file: StoredHashedVideoFile, from_node: Optional[DistributionNodeInfo] = None) -> None:
        """Copy a video from one node to another. If `from_node` is None, copy from the storage node."""
        if file in self.loading or file in self.awaiting_download:
            return
        from_url = from_node.base_url if from_node else settings.public_base_url
        if len(self.loading) < settings.max_parallel_copying_tasks:
            TaskManager.fire_and_forget_task(asyncio.create_task(self._copy_file_task(file, from_url)))
        else:
            self.awaiting_download.schedule(file, from_url)

    async def _copy_file_task(self, file: StoredHashedVideoFile, from_url: str) -> None:
        file.nodes.add_node(self)
        file.nodes.copying = True
        self.loading.add(file)
        url = f'{self.base_url}/api/distributor/copy/{file}'
        data = DistributorCopyFile(from_base_url=from_url, file_size=file.file_size)
        log.info(f"Asking distributor to copy `{file}` from {from_url} to {self.base_url}")
        try:
            code, ret = await HTTPClient.internal_request_node('POST', url, data, timeout=1800)
        except Exception as e:
            file.nodes.remove_node(self)  # This node cannot serve the file
            log.exception(f"Error when copying `{file}` from {from_url} to {self.base_url}")
            raise e
        else:
            if code == 200:
                self.stored_videos.add(file)  # Successfully copied; this node can now serve the file
                log.info(f"Copied `{file}` from {from_url} to {self.base_url}")
            else:
                file.nodes.remove_node(self)  # This node cannot serve the file
                log.error(f"Error copying `{file}` from {from_url} to {self.base_url}, http status {code}")
        finally:
            # Regardless of copying success or failure, always remove the file from `.loading`
            # and set the file's FileNodes `.copying` attribute to `False`.
            self.loading.discard(file)
            file.nodes.copying = False
            if len(self.loading) >= settings.max_parallel_copying_tasks:
                return
            # Check if other files are scheduled for download and if they are, fire off the next copying task.
            try:
                next_file, next_from_url = self.awaiting_download.next()
            except DownloadScheduler.NothingScheduled:
                return
            TaskManager.fire_and_forget_task(asyncio.create_task(self._copy_file_task(next_file, next_from_url)))

    async def _remove_files(self, rem_files: list[FileID], safe: bool = True) -> DistributorDeleteFilesResponse:
        url = f'{self.base_url}/api/distributor/delete'
        data = DistributorDeleteFiles(files=rem_files, safe=safe)
        number = len(rem_files)
        ret: DistributorDeleteFilesResponse
        log.info(f"Asking distributor to remove {number} file(s) from {self.base_url}")
        try:
            code, ret = await asyncio.wait_for(
                HTTPClient.internal_request_node('POST', url, data, DistributorDeleteFilesResponse),
                timeout=60
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

    async def remove_videos(self, files: Iterable[StoredHashedVideoFile], safe: bool = True) -> None:
        hashes_extensions, to_discard = [], {}
        for file in files:
            hashes_extensions.append((file.hash, file.file_extension))
            to_discard[f'{file.hash}{file.file_extension}'] = file
        response_data = await self._remove_files(hashes_extensions, safe=safe)  # calls the distributor's API
        # Only discard files that were actually deleted:
        not_deleted_size = 0
        for file_hash, file_ext in response_data.files_skipped:
            file = to_discard.pop(f'{file_hash}{file_ext}')
            not_deleted_size += file.file_size
        if not_deleted_size:
            log.warning(f"{len(response_data.files_skipped)} file(s) taking up {not_deleted_size} MB "
                        f"could not be deleted from {self.base_url}")
        for file in to_discard.values():
            self.stored_videos.discard(file)
            file.nodes.remove_node(self)
        log.info(f"Removed {len(to_discard)} file(s) (having now {response_data.free_space} MB free space) on {self.base_url}")

    async def unlink_node(self, stop_watching: bool = True) -> None:
        """Do not remove videos on this node, but remove all local references to this node."""
        for file in self.stored_videos:
            file.nodes.remove_node(self)
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
        assert 0 <= settings.dist_free_space_target_ratio <= 1
        if self.free_space_ratio >= settings.dist_free_space_target_ratio:
            log.debug(f"{int(self.free_space)} MB ({round(self.free_space_ratio * 100, 1)} %) "
                      f"of free space available on {self.base_url}")
            return
        target_gain = (settings.dist_free_space_target_ratio * self.total_space - self.free_space) * MEGA
        to_remove, space_gain = [], 0
        sorted_videos = sorted(self.stored_videos, reverse=True)
        while space_gain < target_gain:
            video = sorted_videos.pop()
            to_remove.append(video)
            space_gain += video.file_size
        log.info(f"Trying to purge {len(to_remove)} less popular video(s) "
                 f"to free up {space_gain / MEGA} MB of space at {self.base_url}")
        await self.remove_videos(to_remove)

    def disable(self, stop_watching: bool = True) -> None:
        if not self._enabled:
            raise DistAlreadyDisabled
        self._enabled = False
        if stop_watching:
            self.stop_watching()

    def enable(self) -> None:
        if self._enabled:
            raise DistAlreadyEnabled
        self._enabled = True
        if self.watcher_task is None:
            self.start_watching()


class FileNodes:
    """Node collection for a video file."""
    __slots__ = 'nodes', 'copying'

    def __init__(self) -> None:
        self.nodes: set[DistributionNodeInfo] = set()  # A list of all nodes that have the file or are loading the file.
        self.copying: bool = False  # File is currently being copied to a node.

    def get_least_busy_nodes(self) -> list[DistributionNodeInfo]:
        return sorted(self.nodes)

    def add_node(self, node: DistributionNodeInfo) -> None:
        self.nodes.add(node)

    def remove_node(self, node: DistributionNodeInfo) -> None:
        self.nodes.discard(node)

    def find_good_node(self, file: StoredHashedVideoFile) -> tuple[Optional[DistributionNodeInfo], bool]:
        """
        Find a node that can serve the file and that is not too busy. May also return a node that is currently
        loading the file (if there is no other node).

        :returns (node, False if node currently loads this file)
        """
        nodes = self.get_least_busy_nodes()
        node_loads_file = None
        for node in nodes:
            if node.can_serve:
                if file in node.loading:
                    if node_loads_file is None:
                        node_loads_file = node
                else:
                    # We found a good node.
                    return node, True
        # We don't have a non-busy node.
        if node_loads_file:
            # But we have at least one node that is currently loading the file.
            return node_loads_file, False
        # We don't have any node.
        return None, False


class DistributionController:
    TRACK_MAX_CLIENTS_ACCESSES = 50000

    def __init__(self) -> None:
        self._client_accessed: set[tuple[str, str]] = set()  # tuple of video hash and user's rid
        self._videos_sorted: list[StoredHashedVideoFile] = []
        self._dist_nodes: list[DistributionNodeInfo] = []

    def _reset(self) -> None:
        self._client_accessed.clear()
        for video in self._videos_sorted:
            video.views = 0

    def _find_node(self, matches: Callable[[DistributionNodeInfo], bool],
                   nodes: Optional[Iterable[DistributionNodeInfo]] = None) -> Optional[DistributionNodeInfo]:
        if nodes is None:
            nodes = self._dist_nodes
        for node in nodes:
            if matches(node):
                return node
        return None

    def start_periodic_reset_task(self) -> None:
        async def task() -> None:
            await asyncio.gather(*(dist_node.free_up_space() for dist_node in self._dist_nodes))
            start = timer()
            self._reset()
            log.info(f"Periodic reset task finished (took {(timer() - start):.2f}s)")
        Periodic(task)(interval_seconds=settings.reset_views_every_hours * 3600)

    def count_file_access(self, file: StoredHashedVideoFile, rid: str) -> None:
        """Increment the video views counter if this is the first time the user viewed the video."""
        file_rid = (file.hash, rid)
        if file_rid in self._client_accessed:
            #  Try to count a user only once.
            return
        if len(self._client_accessed) >= self.TRACK_MAX_CLIENTS_ACCESSES:
            # The set should not get too large and the counting doesn't have to be that accurate.
            self._client_accessed.clear()
        self._client_accessed.add(file_rid)
        file.views += 1

    def add_video(self, file: StoredHashedVideoFile) -> None:
        """Used by the FileStorage to notify this class about the file."""
        self._videos_sorted.append(file)

    def remove_video(self, file: StoredHashedVideoFile) -> None:
        self._videos_sorted.remove(file)
        # Remove all copies from the video.
        for node in file.nodes.nodes:
            TaskManager.fire_and_forget_task(asyncio.create_task(node.remove_videos([file], False)))

    def copy_file_to_one_node(self, file: StoredHashedVideoFile) -> Optional[DistributionNodeInfo]:
        # Get a node with tx_load < 0.95, that doesn't already have the file and that has enough space left.
        self._dist_nodes.sort()
        mb_size = file.file_size / MEGA  # to MB

        def _is_viable_target_node(node: DistributionNodeInfo) -> bool:
            return node.can_serve and node not in file.nodes.nodes and node.free_space > mb_size
        to_node = self._find_node(_is_viable_target_node)
        if to_node is None:
            # There is no node the file can be copied to.
            return None

        # Get a node that already has the file.
        def _is_viable_source_node(node: DistributionNodeInfo) -> bool:
            return node.is_enabled and file not in node.loading and node.is_good
        from_node = self._find_node(_is_viable_source_node, nodes=file.nodes.get_least_busy_nodes())
        # When there is no from_node, take this storage node.
        to_node.put_video(file, from_node)
        return to_node

    def add_new_dist_node(self, base_url: str) -> None:
        # Check if we already have this node.
        if self._find_node(lambda node: node.base_url == base_url) is not None:
            log.warning(f"Tried to add distributor node {base_url} again")
            return
        new_node = DistributionNodeInfo(base_url)
        self._dist_nodes.append(new_node)
        new_node.start_watching()
        log.info(f"Added distributor node {base_url}")

    async def remove_dist_node(self, base_url: str) -> None:
        found_node = self._find_node(lambda node: node.base_url == base_url)
        if found_node:
            self._dist_nodes.remove(found_node)
            await found_node.unlink_node()
            log.info(f"Removed dist node {base_url}")
        else:
            log.warning(f"Wanted to remove node {base_url} but did not found.")

    def set_node_state(self, base_url: str, enabled: bool) -> None:
        found_node = self._find_node(lambda node: node.base_url == base_url)
        if not found_node:
            raise UnknownDistURL
        found_node.enable() if enabled else found_node.disable()
        log.info(f"{'En' if enabled else 'Dis'}abled dist node `{base_url}`")

    async def disable_dist_node(self, base_url: str) -> None:
        self.set_node_state(base_url, enabled=False)

    async def enable_dist_node(self, base_url: str) -> None:
        self.set_node_state(base_url, enabled=True)

    def get_dist_node_base_urls(self) -> list[str]:
        return [n.base_url for n in self._dist_nodes]

    def get_nodes_status(self, only_good: bool = False, only_enabled: bool = False) -> dict[str, DistributorStatus]:
        output_dict = {}
        for node in self._dist_nodes:
            if (only_good and not node.is_good) or (only_enabled and not node.is_enabled):
                continue
            assert isinstance(node.status, DistributorStatus)
            output_dict[node.base_url] = node.status
        return output_dict
