from __future__ import annotations
from asyncio.tasks import gather
from collections.abc import Awaitable, Callable, Container, Iterable, Iterator
from functools import partial
from logging import getLogger
from time import time
from timeit import default_timer as timer
from typing import Optional, Union, cast

from videbo import settings
from videbo.client import Client
from videbo.distributor.api.models import DistributorStatus
from videbo.distributor.node import DistributorNode
from videbo.misc.periodic import Periodic
from videbo.misc.task_manager import TaskManager
from .exceptions import DistNodeAlreadyDisabled, UnknownDistURL
from .stored_file import StoredVideoFile


log = getLogger(__name__)


class DistributionController:
    """
    Manages distributor nodes and controls distribution of files to them.

    Can add/remove and enable/disable nodes. When a file is to be distributed
    i.e. copied to (another) distributor node, the `DistributionController`
    determines appropriate source and destination nodes.
    """

    http_client: Client
    _dist_nodes: dict[str, DistributorNode]  # base URL -> `DistributorNode`
    _sort_cache_sec: float
    _last_sort_time: float

    def __init__(
        self,
        node_urls: Iterable[str] = (),
        http_client: Optional[Client] = None,
        sort_cache_sec: float = 5.,
    ) -> None:
        """
        Initializes a `DistributorNode` instance for each url in `node_urls`.

        The periodic distributor cleanup task is launched here as well.
        """
        self._dist_nodes = {}
        self._sort_cache_sec = sort_cache_sec
        self._last_sort_time = 0.
        for url in node_urls:
            self.add_new_dist_node(url)
        self.http_client = Client() if http_client is None else http_client
        Periodic(self.free_up_dist_nodes)(settings.dist_cleanup_freq)

    async def free_up_dist_nodes(self) -> None:
        """
        Executes `free_up_space` for all available distributor nodes.

        The coroutines are run concurrently and their total execution time
        in seconds is logged.
        """
        start = timer()
        await gather(*(node.free_up_space() for node in self.iter_nodes()))
        log.info(f"Freeing up distributors took {(timer() - start):.2f} s")

    def iter_nodes(self) -> Iterator[DistributorNode]:
        """Returns an iterator over the available distributor nodes."""
        return iter(self._dist_nodes.values())

    def sort_nodes(self, force: bool = False) -> None:
        """
        Sorts distributor node dictionary by their TX load (ascending).

        Args:
            force (optional):
                If `False` (default) and the nodes have been recently sorted,
                this method does nothing. If `True` sorting is performed
                regardless of how recently the nodes have been sorted.
        """
        now = time()
        if not force and now < self._last_sort_time + self._sort_cache_sec:
            return
        self._last_sort_time = now
        self._dist_nodes = {
            node.base_url: node
            for node in sorted(self.iter_nodes())
        }

    def filter_nodes(
        self,
        node_matches: Callable[[DistributorNode], bool],
        check_nodes: Optional[Iterable[DistributorNode]] = None,
        exclude_nodes: Container[DistributorNode] = (),
    ) -> Iterator[DistributorNode]:
        """
        Finds all nodes that matches the specified criteria.

        Args:
            node_matches:
                Predicate to match nodes against; must be a callable taking
                a single argument (`DistributorNode`) and returning a `bool`.
            check_nodes (optional):
                If passed an iterable of `DistributorNode` objects, only
                those will be considered; if omitted, all available nodes
                are checked (unless they are contained in `exclude_nodes`).
            exclude_nodes (optional):
                If passed a container of `DistributorNode` objects, nodes
                in that container will not be considered.

        Returns:
            An iterator of `DistributorNode` instances matching the criteria
        """
        if check_nodes is None:
            check_nodes = self.iter_nodes()
        for node in check_nodes:
            if node not in exclude_nodes and node_matches(node):
                yield node

    def get_node(
        self,
        node_matches: Union[str, Callable[[DistributorNode], bool]],
    ) -> Optional[DistributorNode]:
        """
        Finds the first node that matches the specified criteria.

        Args:
            node_matches:
                Predicate to match nodes against; if passed a string, it is
                interpreted as the desired `base_url` of the node; otherwise
                a callable taking a single argument (`DistributorNode`) and
                returning a `bool` is required.

        Returns:
            A `DistributorNode` instance matching the desired criteria or
            `None` if no matching node was found.
        """
        if isinstance(node_matches, str):
            return self._dist_nodes.get(node_matches)
        try:
            return next(self.filter_nodes(node_matches))
        except StopIteration:
            return None

    def remove_from_nodes(self, file: StoredVideoFile) -> None:
        """Schedules tasks to remove `file` from every distributor node."""
        for node in self._dist_nodes.values():
            TaskManager.fire_and_forget(node.remove(file, safe=False))

    def get_node_to_serve(
        self,
        file: StoredVideoFile,
    ) -> tuple[Optional[DistributorNode], bool]:
        """
        Finds a node that can serve the specified `file`.

        If all other nodes are busy, it may also return a node that is
        currently loading the file.

        Returns:
            2-tuple where the first item is the `DistributorNode` instance
            representing the node that can serve the file or `None`, if no
            good node was found, and the second is `True`, if the node has
            the complete file already and `False`, if the node is currently
            still downloading the file.
        """
        self.sort_nodes()
        node_loads_file = None
        can_serve = partial(DistributorNode.can_serve, file=file)
        for node in self.filter_nodes(can_serve):
            if not node.is_loading(file):
                # We found a good node; no need to keep looking
                return node, True
            elif node_loads_file is None:
                # First node that is at least currently loading the file
                node_loads_file = node
        # All nodes are busy, but if at least one currently loads the file,
        # then it will be in `node_loads_file`, otherwise that will be `None`.
        return node_loads_file, False

    def handle_distribution(
        self,
        file: StoredVideoFile,
        storage_tx_load: float,
    ) -> Optional[DistributorNode]:
        """
        Distributes the `file` to another node if necessary and possible.

        If a viable destination node can be found, the search for a node that
        can source it commences. If no distributor can provide the file, it
        may be downloaded from the storage node.
        If no distributor can provide the file and the TX load of the storage
        node exceeds the predefined threshold, the file is not copied.

        Args:
            file: The `StoredVideoFile` representing the file to be copied
            storage_tx_load: The current TX load of the storage node

        Returns:
            The `DistributorNode` instance representing the destination node
            that the `file` is being copied to, if
                - the views for the `file` exceeded the distribution threshold,
                - a viable destination node was found, and
                - a viable source node was available.
            Otherwise `None` is returned.
        """
        if file.num_views < settings.distribution.copy_views_threshold:
            return None
        self.sort_nodes()
        can_receive = partial(DistributorNode.can_receive_copy, file=file)
        to_node = self.get_node(can_receive)
        if to_node is None:
            return None
        can_provide = partial(DistributorNode.can_provide_copy, file=file)
        from_node = self.get_node(can_provide)
        # If there is no `from_node`, storage may serve as source:
        tx_load_threshold = 0.99
        if from_node is None and storage_tx_load > tx_load_threshold:
            log.warning(
                f"Cannot distribute video; TX load {storage_tx_load:.1%} "
                f"above threshold {tx_load_threshold:.1%}"
            )
            return None
        to_node.put_video(file, from_node=from_node)
        # Set view count to 0 to regulate distribution to other nodes:
        file.unique_views.clear()
        return to_node

    def add_new_dist_node(self, base_url: str) -> None:
        """
        Adds a new distributor node to the controller.

        No-op if a node with the provided `base_url` already exists.
        """
        node = self.get_node(base_url)
        if node is not None:
            log.warning(f"Tried to add {node} again")
            return
        node = DistributorNode(base_url)
        self._dist_nodes[base_url] = node
        log.info(f"Added new {node}")

    async def remove_dist_node(self, base_url: str) -> None:
        """
        Removes an existing distributor node from the controller.

        If the node is still enabled, it will be disabled.
        It will be set to a bad state and its file list cleared.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
        """
        node = self._dist_nodes.pop(base_url, None)
        if node is None:
            log.warning(f"Cannot remove unknown distributor at `{base_url}`")
            raise UnknownDistURL(base_url)
        try:
            await node.disable()
        except DistNodeAlreadyDisabled:
            pass
        await node.set_node_state(False)
        log.info(f"Removed {node} from distribution controller")

    async def _enable_or_disable_node(self, base_url: str, enable: bool) -> None:
        """
        Temporarily disables or enables a distributor node.

        Args:
            base_url: The URL of the node to enable/disable
            enable: If `True`, attempts to enable, otherwise disable the node

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
            `DistNodeAlreadyDisabled` if the node is already disabled
            `DistNodeAlreadyEnabled` if the node is already enabled
        """
        found_node = self.get_node(base_url)
        verb = "enable" if enable else "disable"
        if found_node is None:
            log.warning(f"Cannot {verb} unknown distributor at `{base_url}`")
            raise UnknownDistURL(base_url)
        # Call `DistributorNode.enable` or `DistributorNode.disable`:
        call = cast(Callable[[], Awaitable[None]], getattr(found_node, verb))
        await call()
        log.info(f"Successfully {verb}d {found_node}")

    async def disable_dist_node(self, base_url: str) -> None:
        """
        Temporarily disables an active distributor node.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
            `DistNodeAlreadyDisabled` if the node is already disabled
        """
        await self._enable_or_disable_node(base_url, enable=False)

    async def enable_dist_node(self, base_url: str) -> None:
        """
        Enables a previously inactive distributor node.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
            `DistNodeAlreadyEnabled` if the node is already enabled
        """
        await self._enable_or_disable_node(base_url, enable=True)

    def get_nodes_status(
        self,
        only_good: bool = False,
        only_enabled: bool = False,
    ) -> dict[str, DistributorStatus]:
        """
        Returns the `DistributorStatus` objects for the controlled nodes.

        Args:
            only_good (optional):
                If `True`, only nodes with the `is_good` property being `True`
                will be considered; `False` by default.
            only_enabled (optional):
                If `True`, only enabled nodes will be considered;
                `False` by default.

        Returns:
            Dictionary with node base URLs mapped ot the corresponding
            `DistributorStatus` objects.
        """
        output_dict = {}
        for node in self.iter_nodes():
            if (only_good and not node.is_good) or (only_enabled and not node.is_enabled):
                continue
            output_dict[node.base_url] = node.status
        return output_dict
