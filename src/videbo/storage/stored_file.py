from __future__ import annotations
from time import time
from typing import TYPE_CHECKING

from videbo.hashed_file import HashedFile
from videbo.misc.task_manager import TaskManager
if TYPE_CHECKING:
    from videbo.distributor.node import DistributorNode


# TODO: See if we can get rid of the `nodes` back-reference entirely
class StoredVideoFile(HashedFile):
    __slots__ = (
        "size",
        "unique_views",
        "nodes",
        "copying",
    )

    size: int  # in bytes
    unique_views: dict[str, float]  # rid -> timestamp of last view (time sorted)
    nodes: list[DistributorNode]
    copying: bool  # whether it is currently being copied to a node

    def __init__(self, file_hash: str, file_ext: str) -> None:
        super().__init__(file_hash, file_ext)
        self.size = -1
        self.unique_views = {}
        self.nodes = []
        self.copying = False

    def __lt__(self, other: StoredVideoFile) -> bool:
        """Compare videos by their view counters."""
        return self.num_views < other.num_views

    @property
    def num_views(self) -> int:
        """Number of unique views currently tracked by this instance."""
        return len(self.unique_views)

    def register_view_by(self, rid: str) -> None:
        """
        Adds key `rid` with the value of the current timestamp to views.

        If the `rid` key already existed, it is removed first.
        This ensures that the order of key-value-pairs in the views dictionary
        corresponds to the order in which this method is called.
        """
        self.unique_views.pop(rid, None)
        self.unique_views[rid] = time()

    def discard_views_older_than(self, timestamp: float) -> None:
        """
        Removes all entries in the views dictionary with older timestamps.

        Only views with timestamps >= `timestamp` remain in the dictionary.
        """
        for rid, view_timestamp in tuple(self.unique_views.items()):
            if view_timestamp >= timestamp:
                break
            del self.unique_views[rid]

    def remove_from_distributors(self) -> None:
        for node in self.nodes:
            TaskManager.fire_and_forget(node.remove(self, safe=False))
