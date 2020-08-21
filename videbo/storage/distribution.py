import asyncio
from typing import Optional, Set, Tuple, List, Iterable, TYPE_CHECKING
from videbo.misc import TaskManager
from videbo.web import HTTPClient, HTTPResponseError
from videbo.distributor.api.models import DistributorCopyFile, DistributorDeleteFiles,\
    DistributorDeleteFilesResponse, DistributorStatus, DistributorFileList
from videbo.storage import storage_settings, storage_logger
from videbo.storage.exceptions import FileDoesNotExistError
if TYPE_CHECKING:
    from videbo.storage.util import StoredHashedVideoFile


class NothingScheduled(Exception):
    pass


class DownloadScheduler:
    """
    Helper class to collect files that should be downloaded ASAP.

    Additionally, the `in` operator can be used on an instance to determine if a file is already scheduled,
    and the built-in `len(...)` function can be used get the number of files currently scheduled.
    """

    def __init__(self) -> None:
        self.files_from_urls: Set[Tuple[StoredHashedVideoFile, str]] = set()

    def schedule(self, file: 'StoredHashedVideoFile', from_url: str) -> None:
        self.files_from_urls.add((file, from_url))
        storage_logger.info(f"File {file} from {from_url} now scheduled for download")

    def next(self) -> Tuple['StoredHashedVideoFile', str]:
        """
        If any files are left to be downloaded, chose the one with the highest number of views.
        Note that the `StoredHashedVideoFile` class compares objects by their `.views` attribute.
        """
        try:
            tup = max(self.files_from_urls, key=lambda x: x[0])
        except ValueError:
            raise NothingScheduled
        else:
            self.files_from_urls.discard(tup)
            storage_logger.info(f"Next file to download: {tup[0]} from {tup[1]}")
            return tup

    def __contains__(self, item: 'StoredHashedVideoFile') -> bool:
        for file, _ in self.files_from_urls:
            if file == item:
                return True
        return False

    def __len__(self) -> int:
        return len(self.files_from_urls)


class DistributionNodeInfo:
    def __init__(self, base_url: str):
        self.base_url: str = base_url
        self.tx_current_rate: int = 0  # in Mbit/s
        self.tx_max_rate: int = 0  # in Mbit/s
        self.tx_load: float = 0.0  # tx_current_rate / tx_max_rate
        self.free_space: int = 0  # in MB
        self.files_total_size: int = 0  # in MB
        self.stored_videos: Set["StoredHashedVideoFile"] = set()
        self.loading: Set["StoredHashedVideoFile"] = set()  # Node is currently downloading these files.
        self.awaiting_download = DownloadScheduler()  # Files waiting to be downloaded
        self.watcher_task: Optional[asyncio.Task] = None
        self.good: bool = False  # node is reachable

    @property
    def total_space(self) -> float:
        return self.free_space + self.files_total_size

    @property
    def free_space_ratio(self) -> float:
        return self.free_space / self.total_space

    async def start_watching(self):
        async def watcher():
            while True:
                try:
                    url = f"{self.base_url}/api/distributor/status"
                    ret: DistributorStatus
                    status, ret = await HTTPClient.internal_request_node("GET", url, None, DistributorStatus)
                    if status == 200:
                        self.tx_current_rate = ret.tx_current_rate
                        self.tx_max_rate = ret.tx_max_rate
                        self.tx_load = self.tx_current_rate / self.tx_max_rate
                        self.free_space = ret.free_space
                        self.files_total_size = ret.files_total_size
                        await self.set_node_status(True)
                    else:
                        storage_logger.error(f"<Distribution watcher {self.base_url}> http status {status}")
                        await self.set_node_status(False)
                except HTTPResponseError:
                    storage_logger.exception(f"<Distribution watcher {self.base_url}> http error")
                    await self.set_node_status(False)

                await asyncio.sleep(2)

        # File list is loaded before node status switches to good.
        self.watcher_task = asyncio.create_task(watcher())
        TaskManager.fire_and_forget_task(self.watcher_task)

    async def set_node_status(self, new_is_in_good_state: bool):
        if self.good and not new_is_in_good_state:
            await self.unlink_node(False)
        elif not self.good and new_is_in_good_state:
            await self._load_file_list()

        self.good = new_is_in_good_state

    async def _load_file_list(self):
        """Fetch a list of all files that the node currently has."""
        try:
            from .util import FileStorage
            storage = FileStorage.get_instance()
            remove_unknown_files: List[Tuple[str, str]] = []

            url = f"{self.base_url}/api/distributor/files"
            ret: DistributorFileList
            status, ret = await HTTPClient.internal_request_node("GET", url, None, DistributorFileList)
            if status == 200:
                for file_hash, file_extension in ret.files:
                    try:
                        file = await storage.get_file(file_hash, file_extension)
                        self.stored_videos.add(file)
                        file.nodes.add_node(self)
                    except FileDoesNotExistError:
                        storage_logger.info(f"Remove file {file_hash}{file_extension} on {self.base_url} "
                                            f"since file does not exist on storage.")
                        remove_unknown_files.append((file_hash, file_extension))

                storage_logger.info(f"Found {len(self.stored_videos)} files on {self.base_url}")

            else:
                storage_logger.error(f"<Distribution watcher {self.base_url}> http status {status}")

            if len(remove_unknown_files):
                await self._remove_files(remove_unknown_files)

        except HTTPResponseError:
            storage_logger.exception(f"<Distribution watcher {self.base_url}> http error")

    async def stop_watching(self):
        self.watcher_task.cancel()
        self.watcher_task = None

    def put_video(self, file: "StoredHashedVideoFile", from_node: "DistributionNodeInfo" = None) -> None:
        """Copy a video from one node to another.
        If from_node is None, copy from the storage node."""
        if file in self.loading or file in self.awaiting_download:
            return
        from_url = from_node.base_url if from_node else storage_settings.public_base_url
        if len(self.loading) < storage_settings.max_parallel_copying_tasks:
            TaskManager.fire_and_forget_task(asyncio.create_task(self._copy_file_task(file, from_url)))
        else:
            self.awaiting_download.schedule(file, from_url)

    async def _copy_file_task(self, file: 'StoredHashedVideoFile', from_url: str) -> None:
        file.nodes.add_node(self)
        file.nodes.copying = True
        self.loading.add(file)
        url = f"{self.base_url}/api/distributor/copy/{file.hash}{file.file_extension}"
        data = DistributorCopyFile(from_base_url=from_url, file_size=file.file_size)
        try:
            storage_logger.info(f"Asking distributor to copy {file.hash} from {from_url} to {self.base_url}")
            status, ret = await HTTPClient.internal_request_node("POST", url, data, timeout=1800)
        except Exception as e:
            file.nodes.remove_node(self)  # This node cannot serve the file
            storage_logger.exception(f"Error when copying video {file.hash} from {from_url} to {self.base_url}")
            raise e
        else:
            if status == 200:
                self.stored_videos.add(file)  # Successfully copied; this node can now serve the file
                storage_logger.info(f"Copied video {file.hash} from {from_url} to {self.base_url}")
            else:
                file.nodes.remove_node(self)  # This node cannot serve the file
                storage_logger.error(f"Error when copying video {file.hash} from {from_url} to {self.base_url}, "
                                     f"http status {status}")
        finally:
            # Regardless of copying success or failure, always remove the file from `.loading`
            # and set the file's FileNodes `.copying` attribute to `False`.
            self.loading.discard(file)
            file.nodes.copying = False
            if len(self.loading) >= storage_settings.max_parallel_copying_tasks:
                return
            # Check if other files are scheduled for download and if they are, fire off the next copying task.
            try:
                next_file, next_from_url = self.awaiting_download.next()
            except NothingScheduled:
                return
            TaskManager.fire_and_forget_task(asyncio.create_task(self._copy_file_task(next_file, next_from_url)))

    async def _remove_files(self, rem_files: List[Tuple[str, str]]) -> Optional[DistributorDeleteFilesResponse]:
        """
        :argument rem_files List of (hash, file extension)
        """
        url = f"{self.base_url}/api/distributor/delete"
        data = DistributorDeleteFiles(files=rem_files)
        ret: DistributorDeleteFilesResponse
        try:
            status, ret = await asyncio.wait_for(HTTPClient.internal_request_node("POST", url, data,
                                                                                  DistributorDeleteFilesResponse), 60)
        except Exception as e:
            storage_logger.exception(str(e))
            return
        if status == 200:
            self.free_space = ret.free_space
            return ret

    async def remove_videos(self, files: Iterable["StoredHashedVideoFile"]) -> None:
        hashes_extensions, to_discard = [], {}
        for file in files:
            hashes_extensions.append((file.hash, file.file_extension))
            to_discard[f'{file.hash}.{file.file_extension}'] = file
        response_data = await self._remove_files(hashes_extensions)  # calls the distributor's API
        # Only discard files that were actually deleted:
        not_deleted_size = 0
        for file_hash, file_ext in response_data.files_skipped:
            file = to_discard.pop(f'{file_hash}.{file_ext}')
            not_deleted_size += file.file_size
        if not_deleted_size:
            storage_logger.warning(f"{len(response_data.files_skipped)} file(s) taking up {not_deleted_size} MB "
                                   f"could not be deleted from distributor node {self.base_url}")
        for file in to_discard.values():
            self.stored_videos.discard(file)
            file.nodes.remove_node(self)
        storage_logger.info(f"Removed {len(to_discard)} file(s) freeing up {response_data.free_space} MB "
                            f"on distributor node {self.base_url}")

    async def unlink_node(self, stop_watching: bool = True):
        """Do not remove videos on this node, but remove all local references to this node."""
        for file in self.stored_videos:
            file.nodes.remove_node(self)
        self.stored_videos.clear()
        if stop_watching:
            await self.stop_watching()

    async def free_up_space(self) -> None:
        """
        Attempts to remove less popular videos to free-up disk space on the distributor node.
        """
        assert 0 <= storage_settings.dist_free_space_target_ratio <= 1
        if self.free_space_ratio >= storage_settings.dist_free_space_target_ratio:
            storage_logger.debug(f"{self.free_space} MB ({round(self.free_space_ratio * 100, 1)} %) "
                                 f"of free space available at {self.base_url}")
            return
        mega = 1024 * 1024
        target_gain = (storage_settings.dist_free_space_target_ratio * self.total_space - self.free_space) * mega
        to_remove, space_gain = [], 0
        sorted_videos = sorted(self.stored_videos, reverse=True)
        while space_gain < target_gain:
            video = sorted_videos.pop()
            to_remove.append(video)
            space_gain += video.file_size
        storage_logger.info(f"Trying to purge {len(to_remove)} less popular videos "
                            f"to free up {space_gain / mega} MB of space at {self.base_url}")
        await self.remove_videos(to_remove)

    def __lt__(self, other: "DistributionNodeInfo"):
        return self.tx_load < other.tx_load


class FileNodes:
    """Node collection for a video file."""
    __slots__ = "nodes", "copying"

    def __init__(self):
        self.nodes: Set[DistributionNodeInfo] = set()  # A list of all nodes that have the file or are loading the file.
        self.copying: bool = False  # File is currently being copied to a node.

    def get_least_busy_nodes(self) -> List[DistributionNodeInfo]:
        return sorted(self.nodes)

    def add_node(self, node: "DistributionNodeInfo"):
        self.nodes.add(node)

    def remove_node(self, node: "DistributionNodeInfo"):
        self.nodes.discard(node)

    def find_good_node(self, file: "StoredHashedVideoFile") -> Tuple[Optional[DistributionNodeInfo], bool]:
        """Find a node that can serve the file and that is not too busy. May also return a node that is currently
        loading the file (if there is no other node).

        :returns (node, False if node currently loads this file)"""

        nodes = self.get_least_busy_nodes()
        node_loads_file = None
        for node in nodes:
            if node.tx_load < 0.95 and node.good:
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

    def __init__(self):
        self._client_accessed: Set[Tuple[str, str]] = set()  # tuple of video hash and user's rid
        self._videos_sorted: List[StoredHashedVideoFile] = []
        self._dist_nodes: List[DistributionNodeInfo] = []

    def _reset(self) -> None:
        self._client_accessed.clear()
        for video in self._videos_sorted:
            video.views = 0

    def start_periodic_reset_task(self) -> None:
        async def task():
            await asyncio.sleep(storage_settings.reset_views_every_hours)
            self._reset()
            await asyncio.gather(*(dist_node.free_up_space for dist_node in self._dist_nodes))
        TaskManager.fire_and_forget_task(asyncio.create_task(task()))

    def count_file_access(self, file: "StoredHashedVideoFile", rid: str) -> None:
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

    def add_video(self, file: "StoredHashedVideoFile") -> None:
        """Used by the FileStorage to notify this class about the file."""
        self._videos_sorted.append(file)

    def remove_video(self, file: "StoredHashedVideoFile") -> None:
        self._videos_sorted.remove(file)
        # Remove all copies from the video.
        for node in file.nodes.nodes:
            future = node.remove_videos([file])
            TaskManager.fire_and_forget_task(asyncio.create_task(future))

    def copy_file_to_one_node(self, file: "StoredHashedVideoFile") -> Optional[DistributionNodeInfo]:
        # Get a node with tx_load < 0.95, that doesn't already have the file and that has enough space left.
        to_node = None
        self._dist_nodes.sort()
        file_size = file.file_size / 1024 / 1024  # to MB
        for node in self._dist_nodes:
            if node.tx_load < 0.95 and node.good and node not in file.nodes.nodes and node.free_space > file_size:
                to_node = node
                break

        if to_node is None:
            # There is no node where the file can be copied to.
            return None

        # Get a node that already has the file.
        from_node = None
        for node in file.nodes.get_least_busy_nodes():
            if file not in node.loading and node.good:
                from_node = node
                break

        # When there is no from_node, take this storage node.
        to_node.put_video(file, from_node)
        return to_node

    async def add_new_dist_node(self, base_url: str):
        # Check if we already have this node.
        for node in self._dist_nodes:
            if node.base_url == base_url:
                storage_logger.info(f"Tried to add dist node {base_url} again")
                return

        new_node = DistributionNodeInfo(base_url)
        self._dist_nodes.append(new_node)
        await new_node.start_watching()
        storage_logger.info(f"Added dist node {base_url}")

    async def remove_dist_node(self, base_url: str):
        found_node = None
        for node in self._dist_nodes:
            if node.base_url == base_url:
                found_node = node
                break

        if found_node:
            self._dist_nodes.remove(found_node)
            await found_node.unlink_node()
            storage_logger.info(f"Removed dist node {base_url}")
        else:
            storage_logger.warn(f"Wanted to remove node {base_url} but did not found.")

    def get_dist_node_base_urls(self) -> List[str]:
        return [n.base_url for n in self._dist_nodes]
