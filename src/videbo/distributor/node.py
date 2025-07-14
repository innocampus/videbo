from __future__ import annotations
from logging import getLogger
from typing import Optional, TYPE_CHECKING

from videbo import settings
from videbo.exceptions import HTTPClientError
from videbo.misc.constants import HTTP_CODE_OK, MEGA
from videbo.misc.periodic import Periodic
from videbo.misc.task_manager import TaskManager
from videbo.storage.exceptions import (
    DistributionError,
    DistNodeAlreadyDisabled,
    DistNodeAlreadyEnabled,
    DistStatusUnknown,
)
from .api.client import DistributorClient as Client
from .scheduler import DownloadScheduler

if TYPE_CHECKING:
    from videbo.storage.stored_file import StoredVideoFile
    from videbo.types import HashedFileProtocol
    from .api.models import DistributorStatus, DistributorDeleteFilesResponse


log = getLogger(__name__)


class DistributorNode:  # noqa: PLW1641
    """
    Provides an interface for a single distributor node.

    Keeps track of all files hosted on that node, as well as files currently
    being downloaded and scheduled for download by that node.
    """

    http_client: Client
    _status: Optional[DistributorStatus]
    _good: bool
    _enabled: bool
    _files_hosted: set[StoredVideoFile]
    _files_loading: set[StoredVideoFile]
    _files_awaiting_download: DownloadScheduler
    _log_connection_error: bool
    _periodic_watcher: Periodic[[]]

    def __init__(
        self,
        base_url: str,
        enable: bool = True,
        http_client: Optional[Client] = None,
    ) -> None:
        """
        Initializes all attributes and the periodic fetch task.

        Args:
            base_url:
                The base URL of the distributor node
            enable (optional):
                If `True` (default), a periodic task fetching and updating
                the node status is immediately started upon initialization;
                otherwise the node remains disabled and status unknown,
                until the `enable` method is called.
            http_client (optional):
                If omitted, a new HTTP `DistributorClient` is initialized
        """
        if http_client is None:
            http_client = Client(base_url)
        self.http_client = http_client
        self._status = None
        self._good = False  # node is reachable
        self._enabled = False  # node is activated
        self._files_hosted = set()
        self._files_loading = set()
        self._files_awaiting_download = DownloadScheduler()
        self._log_connection_error = True
        self._periodic_watcher = Periodic(self.fetch_dist_status)
        self._periodic_watcher.task_name += f'-{base_url}'
        if enable:
            self.enable()

    def __repr__(self) -> str:
        return f"<Distributor {self.http_client.base_url}>"

    def __eq__(self, other: object) -> bool:
        """Only `True`, if `other` is a `DistributorNode` with the same URL"""
        if not isinstance(other, DistributorNode):
            return NotImplemented
        return self.http_client.base_url == other.http_client.base_url

    def __lt__(self, other: DistributorNode) -> bool:
        """`True`, if the `other` node has a higher `tx_load` than this one"""
        return self.tx_load < other.tx_load

    def __contains__(self, item: StoredVideoFile) -> bool:
        return item in self._files_hosted

    @property
    def base_url(self) -> str:
        return self.http_client.base_url

    @base_url.setter
    def base_url(self, value: str) -> None:
        self.http_client.base_url = value

    @property
    def status(self) -> DistributorStatus:
        """
        The `DistributorStatus` object representing the current node status.

        Raises:
            `DistStatusUnknown` if `fetch_dist_status` has never been called
        """
        if self._status is None:
            raise DistStatusUnknown
        return self._status

    @property
    def is_enabled(self) -> bool:
        """Whether or not the node is currently active."""
        return self._enabled

    @property
    def is_good(self) -> bool:
        """Whether or not the node is currently reachable."""
        return self._good

    @property
    def tx_load(self) -> float:
        """The ratio between the current and maximum TX rate of the node"""
        return self.status.tx_current_rate / self.status.tx_max_rate

    @property
    def free_space(self) -> int:
        """The currently free space on the node in megabytes (rounded down)"""
        return int(self.status.free_space)

    @property
    def total_space(self) -> float:
        """The total space on the node in megabytes"""
        return self.status.free_space + self.status.files_total_size

    @property
    def free_space_ratio(self) -> float:
        """The ratio between free and total space on the node"""
        return round(self.status.free_space / self.total_space, 3)

    @property
    def can_start_downloading(self) -> bool:
        """
        `True`, if the node can start another file download right now.

        This is the case, if the number of current downloads is below the
        configured `max_parallel_copying_tasks`.
        It does _not_ take into account free space to actually host a
        particular file (see `can_host_additional`).
        """
        return len(self._files_loading) < settings.distribution.max_parallel_copying_tasks

    def is_loading(self, file: StoredVideoFile) -> bool:
        """Returns `True`, if the node is currently downloading `file`."""
        return file in self._files_loading

    def scheduled_to_load(self, file: StoredVideoFile) -> bool:
        """Returns `True`, if the node is scheduled to download `file`."""
        return file in self._files_awaiting_download

    def is_distributor_for(self, file: StoredVideoFile) -> bool:
        """
        Returns whether the node is a distributor of the specified `file`.

        A node is considered a distributor, if the node either hosts the
        `file` already, is currently downloading it from another node, or
        has scheduled to download it soon.
        """
        if file in self:
            return True
        return self.is_loading(file) or self.scheduled_to_load(file)

    def can_receive_copy(self, file: StoredVideoFile) -> bool:
        """
        Returns `True`, if the node can become a distributor of `file`.

        Returns `False`, if either of these conditions are present:
            - The node is already a distributor for the specified `file`.
            - The node is disabled or in a bad state.
            - The node has less space available than the `file` requires
            - The status of the node was never fetched.
        """
        if self.is_distributor_for(file):
            log.debug(f"{self} is already a distributor of {file}")
            return False
        if not (self._enabled and self._good):
            log.debug(f"{self} is not available")
            return False
        try:
            enough_space = self.free_space > file.size / MEGA
        except DistStatusUnknown:
            log.error(f"Status unknown for {self}")
            return False
        if not enough_space:
            log.debug(f"{self} does not have enough space")
        return enough_space

    def can_provide_copy(self, file: StoredVideoFile) -> bool:
        """
        Returns `True`, if the node can provide a copy of the specified `file`.

        Returns `False`, if the node does not have a (full) copy of the `file`
        or if it is disabled or in a bad state.
        """
        return (
            file in self
            and self._enabled
            and self._good
            and self.tx_load <= settings.distribution.max_load_file_copy
        )

    def can_serve(self, file: StoredVideoFile) -> bool:
        """`True`, if the node is enabled, under 95 % load and reachable"""
        if not (
            self._enabled
            and self._good
            and self.tx_load <= settings.max_load_file_serving
        ):
            return False
        return file in self._files_hosted or file in self._files_loading

    async def fetch_dist_status(self) -> None:
        """
        Makes a request to the distributor node API to get the current status.

        If the request succeeds, the internal status is updated with the
        response data; if the node state was bad (unreachable) before, it is
        changed to good (reachable) after a successful request.

        If the request fails due to some connection error, the node state is
        set to bad, the error is logged and consecutive connection errors are
        set to no longer be logged.

        If the endpoint responds with anything other than the `HTTP_CODE_OK`,
        the node state is set to bad and the response code is error-logged.
        """
        try:
            code, resp_data = await self.http_client.get_status(
                log_connection_error=self._log_connection_error
            )
        except HTTPClientError as e:
            self._log_connection_error = False
            if self.is_good:
                log.error(f"{e!r} while fetching status of {self}")
                await self.set_node_state(False)
            return
        self._log_connection_error = True
        if code == HTTP_CODE_OK:
            self._status = resp_data
            if not self.is_good:
                log.info(f"Connected to {self} ({self.free_space} MB free)")
                await self.set_node_state(True)
        elif self.is_good:
            log.error(f"HTTP code {code} while fetching status of {self}")
            await self.set_node_state(False)

    async def set_node_state(self, to_good: bool) -> None:
        """
        Switches the node state between good and bad.

        If the state to set is good (`to_good` is `True`) and
        the node state was previously bad, the file list is loaded first,
        _before_ the state is actually set.

        Upon switching from good to bad, the file list is cleared, but the
        periodic status watcher is _not_ stopped.
        """
        if self.is_good and not to_good:
            self._good = False
            self._files_hosted.clear()
        elif not self.is_good and to_good:
            await self._fetch_files_list()
            self._good = True

    async def _fetch_files_list(self) -> None:
        """
        Makes a request to the node's API to get a list of all files it hosts.

        Each file is checked against the storage node and any files that are
        not present on it, are subsequently removed from the distributor;
        each of those cases is logged as a warning.

        A connection error or unexpected response code is logged accordingly.
        """
        from videbo.storage.file_controller import StorageFileController  # noqa: PLC0415
        storage_file_controller = StorageFileController()
        unknown_files = []
        try:
            code, resp_data = await self.http_client.get_files_list()
        except HTTPClientError as e:
            log.error(f"{e!r} while fetching files list of {self}")
            return
        if code != HTTP_CODE_OK:
            log.error(f"HTTP code {code} while fetching files list of {self}")
            return
        for file in resp_data.files:
            stored_file = storage_file_controller.get(file.hash)
            if stored_file is None or stored_file.ext != file.ext:
                log.warning(
                    f"Removing `{file}` from {self} "
                    f"since file does not exist on storage."
                )
                unknown_files.append(file)
            else:
                self._files_hosted.add(stored_file)
        log.info(f"Found {len(self._files_hosted)} files on {self}")
        if unknown_files:
            await self._delete(*unknown_files)

    def put_video(
        self,
        file: StoredVideoFile,
        *,
        from_node: Optional[DistributorNode] = None,
    ) -> None:
        """
        Starts a task o copy a video file from another node to this one.

        If that particular file is already hosted by the node, is currently
        being copied to the node, or is scheduled to be copied to the node,
        this method does nothing.

        If `can_start_downloading` is currently `False`, the file is scheduled
        to be copied later, when the node is able to.

        Args:
            file:
                The `StoredVideoFile` instance representing the file to upload
            from_node (optional):
                If passed a `DistributorNode` instance, the file is downloaded
                from that distributor node; if omitted or `None` (default),
                the storage node serves as the source for the file.
        """
        if self.is_distributor_for(file):
            return
        src = from_node.base_url if from_node else settings.public_base_url
        if self.can_start_downloading:
            TaskManager.fire_and_forget(self._copy(file, from_url=src))
        else:
            self._files_awaiting_download.schedule(file, src)

    async def _copy(self, file: StoredVideoFile, *, from_url: str) -> None:
        """
        Copies a video file from another node to this one.

        The file is added to the `_files_loading` container immediately and
        it remains there for the duration of the request.

        Regardless of errors, after the request the file is removed from the
        `_files_loading` container.

        If the node can download another file right away and there are
        downloads scheduled, another download task is launched at the end.

        Args:
            file:
                The `StoredVideoFile` instance representing the file to upload
            from_url:
                The URL of the node that should serve as a source for the file
        """
        log.info(f"Requesting {self} to download `{file}` from `{from_url}`")
        self._files_loading.add(file)
        try:
            code = await self.http_client.copy(file, from_url=from_url)
        except HTTPClientError as e:
            log.error(f"{e!r} while requesting file download to {self}")
        else:
            if code == HTTP_CODE_OK:
                self._files_hosted.add(file)  # Node can now serve the file
                log.info(f"Downloaded `{file}` from `{from_url}` to {self}")
            else:
                log.error(
                    f"HTTP code {code} while requesting download to {self}"
                )
        finally:
            # Always remove the file from `._files_loading`:
            self._files_loading.discard(file)
            if self.can_start_downloading:
                # Check if other files are scheduled for download and if so,
                # fire off the next download task (callback-style):
                try:
                    file, from_url = self._files_awaiting_download.next()
                except DownloadScheduler.NothingScheduled:
                    return
                TaskManager.fire_and_forget(
                    self._copy(file, from_url=from_url)
                )

    async def _delete(
        self,
        *files: HashedFileProtocol,
        safe: bool = True,
    ) -> DistributorDeleteFilesResponse:
        """
        Makes a request to the node's API to get delete certain files.

        An error during the request or any HTTP status code other than
        `HTTP_CODE_OK` are treated as failure to trigger deletion.

        If successful, the internal `status.free_space` is updated based on
        the response data and the actual number of deleted files is logged.
        Note: The number of deleted files may be less than the number of
        files passed in to be deleted (for various reasons).

        Args:
            *files:
                Files to remove; each object must have both the `hash` and
                the `ext` attribute (both strings)
            safe (optional):
                Whether or not to honor the `last_request_safety_minutes`
                setting, when deleting each individual file; `True` by default

        Returns:
            The `DistributorDeleteFilesResponse` instance received as a
            response from the API endpoint

        Raises:
            `DistributionError` if the request fails or yields a non-OK status
        """
        count = len(files)
        log.info(f"Requesting {self} to delete {count} file(s)")
        try:
            code, resp_data = await self.http_client.delete(*files, safe=safe)
        except HTTPClientError as e:
            log.error(f"{e!r} while requesting file deletion from {self}")
            raise DistributionError from e
        if code == HTTP_CODE_OK:
            self.status.free_space = resp_data.free_space
            num_removed = count - len(resp_data.files_skipped)
            log.info(f"Removed {num_removed} file(s) from {self}")
        else:
            log.error(
                f"HTTP code {code} while requesting file deletion from {self}"
            )
            raise DistributionError
        return resp_data

    async def remove(self, *files: StoredVideoFile, safe: bool = True) -> None:
        """
        Attempts to delete the specified files from the distributor node.

        If an error occurs during the API request, details will be logged.
        Some files may be skipped (not deleted) for various reasons, in which
        case their number and combined size will be warning-logged.

        Args:
            *files:
                Files to remove
            safe (optional):
                Whether or not to honor the `last_request_safety_minutes`
                setting, when deleting each individual file; `True` by default
        """
        to_discard = {str(file): file for file in files}
        try:
            resp_data = await self._delete(*files, safe=safe)
        except DistributionError:
            return  # detailed logging done inside `_delete`
        # Only discard files that were actually deleted:
        not_deleted_size = sum(
            to_discard.pop(str(skipped_file)).size
            for skipped_file in resp_data.files_skipped
        )
        if not_deleted_size > 0:
            log.warning(
                f"{len(resp_data.files_skipped)} file(s) taking up "
                f"{not_deleted_size} MB were not deleted from {self}"
            )
        for file in to_discard.values():
            self._files_hosted.discard(file)
        log.info(
            f"Removed {len(to_discard)} file(s) from {self} "
            f"(now {resp_data.free_space} MB free space left)"
        )

    async def free_up_space(self) -> None:
        """
        Attempts to remove less popular videos to free-up disk space.

        The amount of space to free-up is determined by the current free space
        ratio and the `free_space_target_ratio` setting. If the current ratio
        is not below that target, no files will be deleted.

        The order, in which files are potentially removed is determined by the
        number of unique views; the file with the least number of views
        is removed first.
        """
        if not self._files_hosted or not self._enabled:
            return
        if not self.is_good:
            log.info(f"{self} is not in a good state; skip freeing space.")
            return
        target_ratio = settings.distribution.free_space_target_ratio
        if self.free_space_ratio >= target_ratio:
            log.debug(
                f"{self.free_space} MB ({self.free_space_ratio * 100} %) "
                f"of free space available on {self}"
            )
            return
        target = (target_ratio * self.total_space - self.free_space) * MEGA
        to_remove, space_gain = [], 0
        sorted_videos = sorted(self._files_hosted, reverse=True)
        while space_gain < target:
            video = sorted_videos.pop()
            to_remove.append(video)
            space_gain += video.size
        log.info(
            f"Trying to purge {len(to_remove)} less popular video(s) "
            f"to free up {space_gain / MEGA} MB of space on {self}"
        )
        await self.remove(*to_remove)

    def disable(self, *, stop_watching: bool = True) -> None:
        """
        Disables the node, rendering it temporarily inactive.

        If `stop_watching` is set, the periodic status fetch task is stopped.

        Raises:
            `DistNodeAlreadyDisabled` if the node already was disabled
        """
        if not self._enabled:
            log.warning(f"Already disabled {self}")
            raise DistNodeAlreadyDisabled(self.http_client.base_url)
        self._enabled = False
        if stop_watching:
            self._periodic_watcher.stop()
        log.info(f"Disabled {self}")

    def enable(self) -> None:
        """
        Enables the node, activating it, if it was disabled before.

        If it is not running, the periodic status fetch task is launched.

        Raises:
            `DistNodeAlreadyEnabled` if the node already was enabled
        """
        if self._enabled:
            log.warning(f"Already enabled {self}")
            raise DistNodeAlreadyEnabled(self.http_client.base_url)
        self._enabled = True
        if not self._periodic_watcher.is_running:
            self._periodic_watcher(5, call_immediately=True)
        log.info(f"Enabled {self}")
