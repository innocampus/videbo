import asyncio
from typing import Optional, Set, Tuple, List, TYPE_CHECKING
from livestreaming.misc import TaskManager
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.distributor.api.models import DistributorCopyFile, DistributorDeleteFiles,\
    DistributorDeleteFilesResponse, DistributorStatus, DistributorFileList
from . import storage_settings, storage_logger
from .exceptions import FileDoesNotExistError
if TYPE_CHECKING:
    from .util import StoredHashedVideoFile


class DistributionNodeInfo:
    def __init__(self, base_url: str):
        self.base_url: str = base_url
        self.tx_current_rate: int = 0  # in Mbit/s
        self.tx_max_rate: int = 0  # in Mbit/s
        self.tx_load: float = 0.0  # tx_current_rate / tx_max_rate
        self.free_space: int = 0  # in MB
        self.stored_videos: Set["StoredHashedVideoFile"] = set()
        self.loading: Set["StoredHashedVideoFile"] = set()  # Node is currently downloading these files.
        self.watcher_task: Optional[asyncio.Task] = None
        self.good: bool = False  # node is reachable

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
                        self.good = True
                    else:
                        storage_logger.error(f"<Distribution watcher {self.base_url}> http status {status}")
                        self.good = False
                except HTTPResponseError:
                    storage_logger.exception(f"<Distribution watcher {self.base_url}> http error")
                    self.good = False

                await asyncio.sleep(2)

        await self._load_file_list()
        self.watcher_task = asyncio.create_task(watcher())
        TaskManager.fire_and_forget_task(self.watcher_task)

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
                    except FileDoesNotExistError:
                        remove_unknown_files.append((file_hash, file_extension))

            else:
                storage_logger.error(f"<Distribution watcher {self.base_url}> http status {status}")

            if len(remove_unknown_files):
                await self._remove_files(remove_unknown_files)

        except HTTPResponseError:
            storage_logger.exception(f"<Distribution watcher {self.base_url}> http error")

    async def stop_watching(self):
        self.watcher_task.cancel()
        try:
            await self.watcher_task
        except:
            storage_logger.exception(f"Error in watcher task for <Distribution watcher {self.base_url}>")
        self.watcher_task = None

    def put_video(self, file: "StoredHashedVideoFile", from_node: Optional["DistributionNodeInfo"]) -> None:
        """Copy a video from one node to another.
        If from_node is None, copy from the storage node."""
        if file in self.loading:
            return

        async def copy_file_task():
            try:
                if from_node:
                    from_url = from_node.base_url
                else:
                    from_url = storage_settings.public_base_url
                url = f"{self.base_url}/api/distributor/copy/{file.hash}{file.file_extension}"
                data = DistributorCopyFile(from_base_url=from_url, file_size=file.file_size)
                status, ret = await asyncio.wait_for(HTTPClient.internal_request_node("POST", url, data), 600)
                if status == 200:
                    self.stored_videos.add(file)
                    storage_logger.info(f"Copied video {file.hash} from {from_url} to {self.base_url}")
                else:
                    file.nodes.remove_node(self)
                    storage_logger.error(f"Error when copying video {file.hash} from {from_url} to {self.base_url}, "
                                         f"http status {status}")
            except:
                file.nodes.remove_node(self)
                storage_logger.exception(f"Error when copying video {file.hash} from {from_url} to {self.base_url}")
                raise
            finally:
                self.loading.remove(file)
                file.nodes.copying = False

        self.loading.add(file)
        file.nodes.add_node(self)
        file.nodes.copying = True
        TaskManager.fire_and_forget_task(asyncio.create_task(copy_file_task()))

    async def _remove_files(self, rem_files: List[Tuple[str, str]]):
        """
        :argument rem_files List of (hash, file extension)
        """
        try:
            url = f"{self.base_url}/api/distributor/delete"
            data = DistributorDeleteFiles(files=rem_files)
            ret: DistributorDeleteFilesResponse
            status, ret = await asyncio.wait_for(HTTPClient.internal_request_node("POST", url, data,
                                                                                  DistributorDeleteFilesResponse), 60)
            if status == 200:
                self.free_space = ret.free_space
        except:
            pass

    async def remove_videos(self, files: List["StoredHashedVideoFile"]):
        rem_files = []
        for file in files:
            rem_files.append((file.hash, file.file_extension))
            self.stored_videos.remove(file)
            file.nodes.remove_node(self)

        await self._remove_files(rem_files)

    async def unlink_node(self):
        """Do not remove videos on this node, but remove all local references to this node."""
        for file in self.stored_videos:
            file.nodes.remove_node(self)
        self.stored_videos.clear()
        await self.stop_watching()

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
        self.nodes.remove(node)

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
    PERIODIC_RESET = 4 * 3600

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
            await asyncio.sleep(self.PERIODIC_RESET)
            self._reset()
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
