from __future__ import annotations
from asyncio.tasks import gather
from collections.abc import Callable, Container, Iterable, Iterator
from functools import partial
from logging import getLogger
from timeit import default_timer as timer
from typing import Optional, Union

from videbo import settings
from videbo.client import Client
from videbo.distributor.api.models import DistributorStatus
from videbo.distributor.node import DistributorNode
from videbo.misc import MEGA
from videbo.misc.periodic import Periodic
from .exceptions import UnknownDistURL
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
    _dist_nodes: list[DistributorNode]

    def __init__(
        self,
        node_urls: Iterable[str] = (),
        http_client: Optional[Client] = None,
    ) -> None:
        """
        Initializes a `DistributorNode` instance for each url in `node_urls`.

        The periodic distributor cleanup task is launched here as well.
        """
        self._dist_nodes = []
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
        await gather(*(node.free_up_space() for node in self._dist_nodes))
        log.info(f"Freeing up distributors took {(timer() - start):.2f} s")

    def iter_nodes(self) -> Iterator[DistributorNode]:
        """Returns an iterator over the available distributor nodes."""
        return iter(self._dist_nodes)

    def find_node(
        self,
        matches: Union[str, Callable[[DistributorNode], bool]],
        check_nodes: Optional[Iterable[DistributorNode]] = None,
        exclude_nodes: Container[DistributorNode] = (),
    ) -> Optional[DistributorNode]:
        """
        Finds the first node that matches the specified criteria.

        Args:
            matches:
                Predicate to match nodes against; if passed a string, it is
                interpreted as the desired `base_url` of the node; otherwise
                a callable taking a single argument (`DistributorNode`) and
                returning a `bool` is required.
            check_nodes (optional):
                If passed an iterable of `DistributorNode` objects, only
                those will be considered; if omitted, all available nodes
                are checked (unless they are contained in `exclude_nodes`).
            exclude_nodes (optional):
                If passed a container of `DistributorNode` objects, nodes
                in that container will not be considered.

        Returns:
            A `DistributorNode` instance matching the desired criteria or
            `None` if no matching node was found.
        """
        if check_nodes is None:
            check_nodes = self._dist_nodes
        if isinstance(matches, str):
            url = matches

            def matches(n: DistributorNode) -> bool:
                return n.base_url == url
        for node in check_nodes:
            if node not in exclude_nodes and matches(node):
                return node
        return None

    def copy_file(self, file: StoredVideoFile) -> Optional[DistributorNode]:
        """
        Tries to find a node to copy the `file` to and another to source it.

        If a viable destination node can be found, the search for a node that
        can source it commences. If no distributor can provide the file, it
        will be downloaded from the storage node.

        Args:
            file: The `StoredVideoFile` representing the file to be copied

        Returns:
            The `DistributorNode` instance representing the destination node
            that the `file` is being copied to, if a viable node was found;
            if no viable destination node was found, `None` is returned.
        """
        self._dist_nodes.sort()
        can_host = partial(
            DistributorNode.can_host_additional,
            min_space_mb=file.size / MEGA,
        )
        to_node = self.find_node(can_host, exclude_nodes=file.nodes)
        if to_node is None:
            return None
        has_copy = partial(DistributorNode.can_provide_copy, file=file)
        from_node = self.find_node(has_copy, check_nodes=sorted(file.nodes))
        # If there is no `from_node`, storage will serve as source:
        to_node.put_video(file, from_node)
        return to_node

    def add_new_dist_node(self, base_url: str) -> None:
        """
        Adds a new distributor node to the controller.

        No-op if a node with the provided `base_url` already exists.
        """
        if self.find_node(base_url) is not None:
            log.warning(f"Tried to add distributor node {base_url} again")
            return
        new_node = DistributorNode(base_url)
        self._dist_nodes.append(new_node)
        new_node.start_watching()
        log.info(f"Added distributor node {base_url}")

    async def remove_dist_node(self, base_url: str) -> None:
        """
        Removes an existing distributor node from the controller.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
        """
        found_node = self.find_node(base_url)
        if found_node is None:
            log.warning(f"Cannot remove unknown distributor node `{base_url}`")
            raise UnknownDistURL(base_url)
        self._dist_nodes.remove(found_node)
        await found_node.unlink_node()
        log.info(f"Removed dist node `{base_url}`")

    def _enable_or_disable_node(self, base_url: str, enable: bool) -> None:
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
        found_node = self.find_node(base_url)
        verb = "enable" if enable else "disable"
        if found_node is None:
            log.warning(f"Cannot {verb} unknown distributor node `{base_url}`")
            raise UnknownDistURL(base_url)
        # Call `DistributorNode.enable` or `DistributorNode.disable`:
        getattr(found_node, verb)()
        log.info(f"Successfully {verb}d dist node `{base_url}`")

    def disable_dist_node(self, base_url: str) -> None:
        """
        Temporarily disables an active distributor node.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
            `DistNodeAlreadyDisabled` if the node is already disabled
        """
        self._enable_or_disable_node(base_url, enable=False)

    def enable_dist_node(self, base_url: str) -> None:
        """
        Enables a previously inactive distributor node.

        Raises:
            `UnknownDistURL` if no node with the specified `base_url` is found
            `DistNodeAlreadyEnabled` if the node is already enabled
        """
        self._enable_or_disable_node(base_url, enable=True)

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
        for node in self._dist_nodes:
            if (only_good and not node.is_good) or (only_enabled and not node.is_enabled):
                continue
            output_dict[node.base_url] = node.status
        return output_dict
