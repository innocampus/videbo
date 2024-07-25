from time import time
from typing_extensions import Self

from videbo.hashed_file import HashedFile


class StoredVideoFile(HashedFile):
    __slots__ = ("unique_views", )

    unique_views: dict[str, float]  # rid -> timestamp of last view (time sorted)

    def __init__(self, file_hash: str, file_ext: str, file_size: int) -> None:
        super().__init__(file_hash, file_ext, file_size)
        self.unique_views = {}

    def __lt__(self, other: Self) -> bool:
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
