from abc import ABCMeta, abstractmethod
from collections.abc import AsyncIterator, ValuesView, Iterator, Mapping
from logging import Logger, getLogger
from pathlib import Path
from typing import ClassVar, TypeVar

from aiohttp.web_app import Application

from videbo.client import Client
from videbo.hashed_file import HashedFile
from videbo.misc.constants import MEGA
from videbo.misc.generic_insight import GenericInsightMixin
from videbo.misc.singleton import Singleton
from videbo.models import NodeStatus


F = TypeVar("F", bound=HashedFile)


class FileController(
    Mapping[str, F],
    GenericInsightMixin[F],
    metaclass=Singleton.from_meta(ABCMeta),  # type: ignore[misc]
):
    """
    Generic abstract base class for file controller singletons.

    Publicly functions as a read-only mapping of hashes to file instances.
    Provides a few additional convenience methods, some of which are protected
    to be used by the inheriting classes only.

    !!! note
        The metaclass will enforce that only a single instance of any subclass
        is ever created. Any further attempts at initialization will return
        the instance constructed the first time and are otherwise no-op.
    """

    VIDEO_EXT_WHITELIST: ClassVar[tuple[str, ...]] = ('.mp4', '.webm')
    MAX_NUM_FILES_TO_PRINT: ClassVar[int] = 20

    log: ClassVar[Logger] = getLogger(__name__)

    _http_client: Client
    _files: dict[str, F]    # hash -> object representing a controlled file
    _files_total_size: int  # in bytes, cached for performance

    def __init__(self) -> None:
        """
        Initializes the relevant attributes and starts a `Client`.

        Globs the controller's `files_dir` and collects information on every
        file inside it that passes the `load_file_predicate` method.
        """
        self._http_client = Client()
        self._files = {}
        self._files_total_size = 0
        self._load_file_list()
        self.log.info(f"Initialized {self} singleton")

    def __getitem__(self, file_hash: str) -> F:
        """
        Returns the file with the specified hash, if the controller has it.
        Raises a `KeyError` otherwise.
        """
        return self._files[file_hash]

    def __iter__(self) -> Iterator[str]:
        """
        Returns an iterator over the hashes of the controlled files.
        """
        return iter(self._files.keys())

    def __len__(self) -> int:
        """Returns the total number of controlled files."""
        return len(self._files)

    def __str__(self) -> str:
        """Returns the class name."""
        return self.__class__.__name__  # type: ignore[no-any-return]

    def iter_files(self) -> ValuesView[F]:
        """
        Returns a view (i.e. a sized collection) of the controlled files.

        This method is just an alias for the inherited mapping `values` method.
        """
        return self.values()

    @property
    def files_total_size_mb(self) -> float:
        """Total size taken up by the controlled files in MB."""
        return self._files_total_size / MEGA

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        """
        Context generator for the `aiohttp` web application.

        Args:
            _app: Not considered (required parameter for the application)

        Returns:
            Asynchronous iterator that instantiates the file controller
            singleton (if it has not been instantiated yet) before yielding.
            Does not do any cleanup code.
        """
        cls()
        yield
        # Possible cleanup code can be added by a subclass here.

    def _add_file(self, file: F) -> None:
        self._files[file.hash] = file
        self._files_total_size += file.size

    def _remove_file(self, file_hash: str) -> None:
        file = self._files.pop(file_hash)
        self._files_total_size -= file.size

    def load_file_predicate(self, file_path: Path) -> bool:
        """
        Returns `True` if the specified file path should be loaded.

        Args:
            file_path:
                Must be a valid path to an existing file inside the controller's
                `files_dir` directory.

        Returns:
            `True` if the `file_path` belongs to a file that should be loaded.
            `False` otherwise.
        """
        if len(file_path.suffixes) != 1:
            self.log.warning(f"File with invalid/no extension: {file_path}")
            return False
        if file_path.suffix not in self.VIDEO_EXT_WHITELIST:
            self.log.warning(f"File extension not allowed: {file_path}")
            return False
        return True

    def _load_file_list(self) -> None:
        """Load information about all files saved on the node."""
        if not self.files_dir.is_dir():
            raise NotADirectoryError(self.files_dir)
        file_cls = self.get_type_arg()
        for file_path in self.files_dir.glob('**/*'):
            if not file_path.is_file():
                continue
            if not self.load_file_predicate(file_path):
                continue
            count = len(self)
            if count < self.MAX_NUM_FILES_TO_PRINT:
                self.log.info(f"Adding file {file_path} to {self}")
            elif count == self.MAX_NUM_FILES_TO_PRINT:
                self.log.info("Skip logging other files being added...")
            self._add_file(file_cls.from_path(file_path))
        self.log.info(f"{self} found {len(self)} files")

    @property
    @abstractmethod
    def files_dir(self) -> Path:
        """The directory where all controlled files reside."""
        raise NotImplementedError

    @abstractmethod
    def get_path(self, file_name: str, *, temp: bool = False) -> Path:
        """
        Returns the path the controller would use for the specified file name.

        Args:
            file_name:
                Self-explanatory
            temp:
                If `True` the path to a temporary file with the specified name
                will be returned.

        Returns:
            The path from which the controller would read a file with the
            specified name. Need not be the canonical or absolute path. At the
            time of calling, the file may or may not exist on the file system.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_status(self) -> NodeStatus:
        """
        Returns the status of the entire node.

        Returns:
            An instance of the `NodeStatus` model.
        """
        raise NotImplementedError
