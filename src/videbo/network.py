from __future__ import annotations
import re
from asyncio import CancelledError, Task, create_task, sleep
from contextlib import suppress
from enum import Enum
from logging import Logger, getLogger
from pathlib import Path
from time import time
from typing import ClassVar, Optional, TYPE_CHECKING

from pydantic import BaseModel

from videbo import settings
from videbo.client import Client
from videbo.exceptions import HTTPClientError, UnknownServerStatusFormatError
from videbo.misc.constants import HTTP_CODE_OK, MEGA

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from aiohttp.web_app import Application

    from videbo.models import NodeStatus


log = getLogger(__name__)

HTML_PATTERN = re.compile(r"\s*<(!DOCTYPE|html).*>.*")

PROC_NET_DEV = Path("/proc/net/dev")
# All regex group names except `name` must match
# the field names of the `InterfaceStats` model exactly!
INTERFACE_STATS_PATTERN = re.compile(
    r"(?:^\s*(?P<name>[^\s/]{1,15}):\s+)?"
    r"(?P<bytes>\d+)\s+"
    r"(?P<packets>\d+)\s+"
    r"(?P<errs>\d+)\s+"
    r"(?P<drop>\d+)\s+"
    r"(?P<fifo>\d+)\s+"
    r"(?P<frame>\d+)\s+"
    r"(?P<compr>\d+)\s+"
    r"(?P<multicast>\d+)"
)


class InterfaceStats(BaseModel):
    bytes: int = 0
    throughput: float = 0.  # bytes per second
    packets: int = 0
    drop: int = 0
    errs: int = 0
    fifo: int = 0
    frame: int = 0
    compr: int = 0
    multicast: int = 0

    def update_throughput(self, bytes_before: int, interval_seconds: float) -> None:
        self.throughput = (self.bytes - bytes_before) / interval_seconds

    class Config:
        validate_assignment = True


class NetworkInterface(BaseModel):
    name: str
    rx: InterfaceStats = InterfaceStats()
    tx: InterfaceStats = InterfaceStats()

    class Config:
        validate_assignment = True


class ServerType(str, Enum):
    apache = "Apache2"
    nginx = "nginx"
    unknown = "unknown"


class StubStatus(BaseModel):
    reading: int = 0
    waiting: int = 0
    writing: int = 0
    server_type: ServerType = ServerType.unknown

    class Config:
        validate_assignment = True


class NetworkInterfaces:
    """
    NetworkInterfaces: a class to monitor your interfaces
    """
    # make _interfaces a singleton since they can be shared on one system
    _instance: ClassVar[Optional[NetworkInterfaces]] = None
    _interfaces: dict[str, NetworkInterface]
    _last_time_network_proc: float
    # TODO(daniil-berg): Use a custom `Periodic` instead of the `_fetch_task`.
    #                    https://github.com/innocampus/videbo/issues/24
    _fetch_task: Optional[Task[None]]
    _server_status: StubStatus

    def __init__(self) -> None:
        self.http_client: Client = Client()
        self._interfaces = {}
        self._last_time_network_proc = 0.0
        self._fetch_task = None
        self._server_status = StubStatus()

    @staticmethod
    def get_instance() -> NetworkInterfaces:
        if NetworkInterfaces._instance is None:
            NetworkInterfaces._instance = NetworkInterfaces()
        return NetworkInterfaces._instance

    @classmethod
    async def app_context(cls, _app: Application) -> AsyncIterator[None]:
        cls.get_instance().start_fetching()
        yield
        await cls.get_instance().stop_fetching()

    @property
    def is_fetching(self) -> bool:
        return self._fetch_task is not None

    def _fetch_proc_info(self) -> None:
        """Fetching data from /proc/net/dev"""
        interval_seconds = time() - self._last_time_network_proc
        with PROC_NET_DEV.open('r') as f:
            for line in f:
                try:
                    rx_match, tx_match = re.finditer(INTERFACE_STATS_PATTERN, line)
                except ValueError:
                    continue
                name = rx_match.group("name")
                if name == "lo":
                    continue
                interface = self._interfaces.setdefault(name, NetworkInterface(name=name))
                rx_bytes_before, tx_bytes_before = interface.rx.bytes, interface.tx.bytes
                interface.rx = InterfaceStats.parse_obj(rx_match.groupdict())
                interface.tx = InterfaceStats.parse_obj(tx_match.groupdict())
                if self._last_time_network_proc:
                    interface.rx.update_throughput(rx_bytes_before, interval_seconds)
                    interface.tx.update_throughput(tx_bytes_before, interval_seconds)
        self._last_time_network_proc += interval_seconds

    def _update_apache_status(self, lines_of_text: list[str]) -> bool:
        scan = False
        text_to_scan = ""
        for line in lines_of_text:
            if re.match(r".*<pre>[A-Z_.]+", line):
                scan = True
            if scan:
                text_to_scan += line
            if scan and re.match(r"[A-Z_.]*</pre>.*", line):
                self._server_status.writing = text_to_scan.count("W") - 1
                self._server_status.reading = text_to_scan.count("R")
                self._server_status.waiting = text_to_scan.count("_")
                return True
        return False

    def _update_nginx_status(self, text: str) -> bool:
        match = re.match(
            r"Reading:\s*(?P<reading>\d+)\s+"
            r"Writing:\s*(?P<writing>\d+)\s+"
            r"Waiting:\s*(?P<waiting>\d+)\s*",
            text
        )
        if match is None:
            return False
        self._server_status.writing = int(match.group("writing")) - 1
        self._server_status.reading = int(match.group("reading"))
        self._server_status.waiting = int(match.group("waiting"))
        return True

    async def _fetch_server_status(self, url: str) -> None:
        http_code: int
        response_data: bytes
        try:
            http_code, response_data = await self.http_client.request("GET", url)
        except (HTTPClientError, ConnectionRefusedError) as e:
            log.warning("Error requesting %s: %s", url, repr(e))
            return
        if http_code != HTTP_CODE_OK:
            log.warning(f"unexpected response from {url} (return code {http_code})")
            return
        text = response_data.decode("utf-8")
        lines = text.split("\n")
        if re.match(HTML_PATTERN, text):
            self._server_status.server_type = ServerType.apache
            updated = self._update_apache_status(lines)
        else:
            self._server_status.server_type = ServerType.nginx
            try:
                updated = self._update_nginx_status(lines[3])
            except IndexError:
                err = UnknownServerStatusFormatError(
                    f"Response text from '{url}' has less than 4 lines: "
                    f"Unexpected nginx stub status format"
                )
                log.error(str(err))
                raise err from None
        if not updated:
            err = UnknownServerStatusFormatError(
                f"Data received from '{url}' does not match the "
                f"expected server type: {self._server_status.server_type})"
            )
            log.error(str(err))
            raise err

    async def _fetch_loop(self) -> None:
        while True:
            try:
                self._fetch_proc_info()
            except Exception as e:
                log.exception(f"{e} in network _fetch_proc_info")
            if settings.webserver.status_page:
                try:
                    await self._fetch_server_status(settings.webserver.status_page)
                except Exception as e:
                    log.exception(f"{e} in network _fetch_server_status")
            await sleep(settings.network_info_fetch_interval)

    def start_fetching(self) -> None:
        """Starts the fetching process"""
        if self._fetch_task is not None:
            return
        self._fetch_task = create_task(self._fetch_loop())
        log.info("Started fetching network resources info")

    async def stop_fetching(self) -> None:
        """Stops the fetching process"""
        if self._fetch_task is not None:
            log.debug("Stopping network resource info fetch task...")
            self._fetch_task.cancel()
            with suppress(CancelledError):
                await self._fetch_task
            self._fetch_task = None
            log.info("Stopped fetching network resources info")

    def get_first_interface(self) -> Optional[NetworkInterface]:
        try:
            return next(iter(self._interfaces.values()))
        except StopIteration:
            return None

    def get_tx_current_rate(self, interface_name: Optional[str] = None) -> Optional[float]:
        """
        Returns the current TX throughput in bytes per second.

        Args:
            interface_name (optional):
                If provided, the stats are taken from the interface with the
                specified name; if no interface with that name is found,
                `None` is returned. If omitted or `None` (default), the first
                listed interface is used; if no interface at all was found,
                `None` is returned.

        Returns:
            The TX rate of the chosen interface in bytes per second or `None`,
            if no (matching) interface was found.
        """
        if interface_name is None:
            interface = self.get_first_interface()
        else:
            interface = self._interfaces.get(interface_name)
        if interface is None:
            return None
        return interface.tx.throughput

    def get_tx_load(self, interface_name: Optional[str] = None) -> Optional[float]:
        """
        Returns the current TX load of the network interface.

        Args:
            interface_name (optional): same as in `get_tx_current_rate`

        Returns:
            The TX load of the chosen interface as a ratio of the current TX
            throughput in Mbit/s to the configured `tx_max_rate_mbit` or
            `None`, if no (matching) interface was found.
        """
        current_rate = self.get_tx_current_rate(interface_name=interface_name)
        if current_rate is None:
            return None
        return (current_rate * 8 / 1_000_000) / settings.tx_max_rate_mbit

    # TODO(daniil-berg): Move this to the `NodeStatus` class.
    #                    https://github.com/innocampus/videbo/issues/23
    def update_node_status(self, status_obj: NodeStatus, logger: Logger = log) -> None:
        """Updates a given `NodeStatus` (subclass) instance with network interface information"""
        if self._server_status is not None:
            status_obj.current_connections = self._server_status.writing
        interface = self.get_first_interface()
        if interface is None:
            logger.error("No network interface found!")
        else:
            status_obj.tx_current_rate = round(interface.tx.throughput * 8 / 1_000_000, 2)
            status_obj.rx_current_rate = round(interface.rx.throughput * 8 / 1_000_000, 2)
            status_obj.tx_total = round(interface.tx.bytes / MEGA, 2)
            status_obj.rx_total = round(interface.rx.bytes / MEGA, 2)
