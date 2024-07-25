from asyncio.locks import Event
from asyncio.tasks import wait_for
from time import time

from videbo.hashed_file import HashedFile
from .distributed_file import DistributedVideoFile


class CopyingVideoFile(HashedFile):
    __slots__ = (
        "source_url",
        "time_started",
        "loaded_bytes",
        "_finished",
    )

    source_url: str
    time_started: float
    loaded_bytes: int
    _finished: Event  # Set when the file was downloaded completely

    def __init__(
        self,
        file_hash: str,
        file_ext: str,
        file_size: int,
        source_url: str,
    ) -> None:
        super().__init__(file_hash, file_ext, file_size)
        self.source_url = source_url
        self.time_started = time()
        self.loaded_bytes = 0
        self._finished = Event()

    async def wait_until_finished(self, timeout: float = 60.) -> None:
        """
        Waits until the file has finished copying.

        Args:
            timeout: Timeout in seconds.

        Raises:
            asyncio.TimeoutError:
                If the file has not finished copying after `timeout` seconds.
        """
        await wait_for(self._finished.wait(), timeout)

    def as_finished(self) -> DistributedVideoFile:
        """
        Declares the copying finished.

        Returns:
            A corresponding `DistributedVideoFile` instance.
        """
        self._finished.set()
        return DistributedVideoFile(self.hash, self.ext, self.loaded_bytes)
