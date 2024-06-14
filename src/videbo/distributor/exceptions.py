from __future__ import annotations
from typing import TYPE_CHECKING

from videbo.exceptions import VideboBaseException
if TYPE_CHECKING:
    from .copying_file import CopyingVideoFile
    from .distributed_file import DistributedVideoFile


class DistributorError(VideboBaseException):
    pass



class CopyFileError(DistributorError):
    pass


class NotEnoughSpace(CopyFileError):
    def __init__(self, required: float, free: float) -> None:
        super().__init__(f"{free=:.1f} MB, {required=:.1f} MB")


class UnexpectedFileSize(CopyFileError):
    def __init__(self, file: CopyingVideoFile) -> None:
        super().__init__(
            f"Loaded {file.loaded_bytes} bytes, "
            f"but expected {file.expected_bytes} bytes."
        )


class NotSafeToDelete(DistributorError):
    def __init__(self, file: DistributedVideoFile) -> None:
        super().__init__(
            f"This file was last requested at {file.last_requested}: {file}"
        )


class NoSuchFile(DistributorError):
    def __init__(self, file_hash: str) -> None:
        super().__init__(f"Unknown file hash {file_hash}")


class TooManyWaitingClients(DistributorError):
    def __init__(self, file: CopyingVideoFile, num_clients: int) -> None:
        super().__init__(
            f"{num_clients} clients are already waiting for file {file}"
        )
