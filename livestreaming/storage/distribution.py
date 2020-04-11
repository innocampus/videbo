from typing import Optional, Set, Tuple, List, TYPE_CHECKING
from . import storage_settings
if TYPE_CHECKING:
    from .util import StoredHashedVideoFile


class DistributionNodeInfo:
    def __init__(self, base_url: str):
        self.base_url: str = base_url
        self.tx_current_rate: int = 0  # in Mbit/s
        self.tx_max_rate: int = 0  # in Mbit/s
        self.free_space: int = 0  # in MB
        self.total_space: int = 0  # in MB
        self.stored_videos: Set["StoredHashedVideoFile"] = set()
        self.loading: Set["StoredHashedVideoFile"] = set()

    async def put_video(self, file: "StoredHashedVideoFile", from_node: Optional["DistributionNodeInfo"]) -> None:
        """Copy a video from one node to another.
        If from_node is None, copy from the storage node."""
        if file in self.loading:
            return

    async def remove_video(self, file: "StoredHashedVideoFile"):
        pass

    def __lt__(self, other: "DistributionNodeInfo"):
        return (self.tx_current_rate / self.tx_max_rate) < (other.tx_current_rate / self.tx_max_rate)

    async def _get_initial_file_list(self) -> None:
        """Fetch a list of all files that the node currently has."""
        pass


class FileNodes:
    """Node collection for a video file."""
    def __init__(self):
        self.nodes: Set[DistributionNodeInfo] = set()

    def get_least_busy_node(self) -> Optional[DistributionNodeInfo]:
        return min(self.nodes, default=None)

    def add_node(self, node: "DistributionNodeInfo"):
        self.nodes.add(node)

    def remove_node(self, node: "DistributionNodeInfo"):
        self.nodes.remove(node)


class DistributionController:
    TRACK_MAX_CLIENTS_ACCESSES = 50000

    def __init__(self):
        self._client_accessed: Set[Tuple[str, str]] = set()  # tuple of video hash and user's rid
        self._videos_sorted: List[StoredHashedVideoFile] = []

    def _reset(self) -> None:
        self._client_accessed.clear()
        for video in self._videos_sorted:
            video.views = 0

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

