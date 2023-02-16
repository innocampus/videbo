from __future__ import annotations
from asyncio.tasks import gather
from collections.abc import Callable, Container, Iterable
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
    def __init__(
        self,
        node_urls: Iterable[str] = (),
        http_client: Optional[Client] = None,
    ) -> None:
        self._dist_nodes: list[DistributorNode] = []
        for url in node_urls:
            self.add_new_dist_node(url)
        self.http_client: Client = Client() if http_client is None else http_client
        Periodic(self.free_up_dist_nodes)(settings.dist_cleanup_freq)

    def _find_node(
        self,
        matches: Union[str, Callable[[DistributorNode], bool]],
        check_nodes: Optional[Iterable[DistributorNode]] = None,
        exclude_nodes: Container[DistributorNode] = (),
    ) -> Optional[DistributorNode]:
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

    async def free_up_dist_nodes(self) -> None:
        start = timer()
        await gather(*(node.free_up_space() for node in self._dist_nodes))
        log.info(f"Freeing up distributors took {(timer() - start):.2f} s")

    def copy_file_to_one_node(self, file: StoredVideoFile) -> Optional[DistributorNode]:
        # Get a node with tx_load < 0.95, that doesn't already have the file and that has enough space left.
        self._dist_nodes.sort()
        matches = partial(
            DistributorNode.can_host_additional,
            min_space_mb=file.size / MEGA,
        )
        to_node = self._find_node(matches, exclude_nodes=file.nodes)
        if to_node is None:
            # There is no node the file can be copied to.
            return None
        # Get a node that already has the file.
        matches = partial(
            DistributorNode.can_provide_copy,
            file=file,
        )
        from_node = self._find_node(matches, check_nodes=sorted(file.nodes))
        # When there is no `from_node`, storage will serve as source.
        to_node.put_video(file, from_node)
        return to_node

    def add_new_dist_node(self, base_url: str) -> None:
        # Check if we already have this node.
        if self._find_node(base_url) is not None:
            log.warning(f"Tried to add distributor node {base_url} again")
            return
        new_node = DistributorNode(base_url)
        self._dist_nodes.append(new_node)
        new_node.start_watching()
        log.info(f"Added distributor node {base_url}")

    async def remove_dist_node(self, url: str) -> None:
        found_node = self._find_node(url)
        if found_node is None:
            log.warning(f"Cannot remove unknown distributor node `{url}`")
            raise UnknownDistURL(url)
        self._dist_nodes.remove(found_node)
        await found_node.unlink_node()
        log.info(f"Removed dist node `{url}`")

    def disable_dist_node(self, url: str) -> None:
        found_node = self._find_node(url)
        if found_node is None:
            log.warning(f"Cannot disable unknown distributor node `{url}`")
            raise UnknownDistURL(url)
        found_node.disable()
        log.info(f"Disabled dist node `{url}`")

    def enable_dist_node(self, url: str) -> None:
        found_node = self._find_node(url)
        if found_node is None:
            log.warning(f"Cannot enable unknown distributor node `{url}`")
            raise UnknownDistURL(url)
        found_node.enable()
        log.info(f"Enabled dist node `{url}`")

    def get_dist_node_base_urls(self) -> list[str]:
        return [n.base_url for n in self._dist_nodes]

    def get_nodes_status(self, only_good: bool = False, only_enabled: bool = False) -> dict[str, DistributorStatus]:
        output_dict = {}
        for node in self._dist_nodes:
            if (only_good and not node.is_good) or (only_enabled and not node.is_enabled):
                continue
            assert isinstance(node.status, DistributorStatus)
            output_dict[node.base_url] = node.status
        return output_dict
