from __future__ import annotations
import os
from hashlib import sha256
from pathlib import Path
from tempfile import mkstemp
from types import TracebackType
from typing import BinaryIO, ClassVar, Literal, Optional, TypeVar

from videbo.exceptions import PendingWriteOperationError
from videbo.misc.functions import move_file, run_in_default_executor
from videbo.types import PathT


__all__ = [
    "TempFile",
]

E = TypeVar("E", bound=BaseException)


class TempFile:
    """
    Allows writing temporary files in a non-blocking way.

    Used for files being uploaded, but not permanently stored yet.
    """

    file_name_prefix: ClassVar[str] = "upload_"

    @classmethod
    def create(
        cls,
        directory: PathT,
        open_mode: Optional[Literal["wb", "rb"]] = None,
    ) -> TempFile:
        """
        Creates a temp. file in the specified `directory`.

        If `open_mode` is given, opens the new file in that mode.
        Returns a corresponding instance of this class.
        """
        fd, path = mkstemp(prefix=cls.file_name_prefix, dir=str(directory))
        return cls(fd, Path(path), open_mode=open_mode)

    def __init__(
        self,
        file_descriptor: int,
        path: PathT,
        open_mode: Optional[Literal["wb", "rb"]] = None,
    ) -> None:
        """
        Initializes the internal hash container and size counter.

        Also sets the permissions on the specified file `path` to `644`.
        If `open_mode` is given, opens the `file_descriptor` in that mode.
        """
        self._file_descriptor = file_descriptor
        self._path = Path(path)
        self._path.chmod(0o644)  # make readable for other user
        self._is_writing: bool = False
        self._file_handle: Optional[BinaryIO] = None
        self._hash = sha256()
        self._size: int = 0
        if open_mode:
            self.open(open_mode)

    @property
    def path(self) -> Path:
        """Current location of the file."""
        return self._path

    @property
    def is_open(self) -> bool:
        """Whether the underlying file descriptor is currently open."""
        return self._file_handle is not None

    @property
    def digest(self) -> str:
        """Current hex-digest of the data in the file."""
        return self._hash.hexdigest()

    @property
    def size(self) -> int:
        """Current size of the file in bytes."""
        return self._size

    def open(self, mode: Literal["wb", "rb"] = "wb") -> TempFile:
        """
        Opens the file descriptor in binary read or write mode.

        Also (re-)sets the internal hash container and size counter.
        """
        self._file_handle = os.fdopen(self._file_descriptor, mode)
        self._hash = sha256()
        self._size = 0
        return self

    async def close(self) -> None:
        """Closes the underlying file handle via another thread."""
        if self._file_handle is None:
            return
        await run_in_default_executor(self._file_handle.close)
        self._file_handle = None

    async def __aenter__(self) -> TempFile:
        """
        Allows using the instance as a context manager.

        If the underlying file descriptor is not open, an error is raised.
        Returns the instance itself.
        """
        if not self.is_open:
            raise RuntimeError("File is not open")
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[E]],
        exc_val: Optional[E],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Allows using the instance as a context manager.

        Closes the open file handle via another thread.
        """
        await self.close()

    def _write(self, data: bytes) -> None:
        """Writes `data` to file, updates hash, and increments size."""
        if self._file_handle is None:
            raise RuntimeError("File is not open")
        self._file_handle.write(data)
        self._hash.update(data)
        self._size += len(data)

    async def write(self, data: bytes) -> None:
        """Writes `data` to file in another thread and updates the hash."""
        if self._is_writing:
            raise PendingWriteOperationError
        self._is_writing = True
        await run_in_default_executor(self._write, data)
        self._is_writing = False

    async def delete(self) -> None:
        """Deletes the file via another thread."""
        await run_in_default_executor(self._path.unlink, True)

    async def persist(
        self,
        file_ext: Optional[str] = None,
        file_name_stem: Optional[str] = None,
        target_dir: Optional[PathT] = None,
    ) -> Path:
        """
        Moves/renames the file to a permanent location via another thread.

        If the new path already exists, the file at the current path of this
        `TempFile` instance is simply deleted.
        The current path of the instance is then set to the new path.

        Args:
            file_ext (optional):
                If provided, it is appended as the suffix to the file name;
                the extension should begin with a dot.
            file_name_stem (optional):
                If provided, it is used as the stem of the file path;
                otherwise the hash digest of the file contents is used.
            target_dir (optional):
                If provided, the file is moved to that directory;
                otherwise it is just renamed inside its current directory.

        Returns:
            The new path of the persisted file.
        """
        if self._is_writing:
            raise PendingWriteOperationError
        target_path = self._path.with_stem(
            self.digest if file_name_stem is None else file_name_stem
        )
        if file_ext:
            target_path = target_path.with_suffix(file_ext)
        if target_dir:
            target_path = Path(target_dir, target_path.name)
        self._is_writing = True  # Don't allow any additional writes.
        await run_in_default_executor(move_file, self._path, target_path)
        self._path = target_path
        return target_path
