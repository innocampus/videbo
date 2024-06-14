from time import time

from videbo.hashed_file import HashedFile


class DistributedVideoFile(HashedFile):
    __slots__ = ("_last_requested", "size")

    size: int  # in bytes
    _last_requested: int  # UNIX timestamp (seconds); -1 means never/unknown

    def __init__(self, file_hash: str, file_ext: str, file_size: int) -> None:
        super().__init__(file_hash, file_ext)
        self._last_requested = -1
        self.size = file_size

    @property
    def last_requested(self) -> int:
        return self._last_requested

    def set_requested_time(self) -> None:
        self._last_requested = int(time())
