import asyncio
import re
import socket
from logging import Logger
from time import time
from typing import Optional, Dict, List, Union

from videbo.web import HTTPClient, HTTPResponseError
from videbo.misc import MEGA, TaskManager
from videbo.models import NodeStatus


class PureDataType:
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # TODO: make it pretty
        return f"{self.__dict__}"


class NetworkInterface(PureDataType):
    """
    Pure data holder for NetworkInterfaces
    """
    def __init__(self, name: str):
        self.name = name
        self.rx_bytes = 0
        self.rx_throughput: float = 0  # bytes per second
        self.rx_packets = 0
        self.rx_drops = 0
        self.rx_errors = 0
        self.rx_fifo = 0
        self.rx_frame = 0
        self.rx_compr = 0
        self.rx_multicast = 0
        self.tx_bytes = 0
        self.tx_throughput: float = 0  # bytes per second
        self.tx_packets = 0
        self.tx_drop = 0
        self.tx_errs = 0
        self.tx_fifo = 0
        self.tx_frame = 0
        self.tx_compr = 0
        self.tx_multicast = 0


class StubStatus(PureDataType):
    """
    Pure data holder for server status
    """
    def __init__(self):
        self.reading: int = 0
        self.waiting: int = 0
        self.writing: int = 0
        self.server_type: str = "Unknown"


class UnknownServerStatusFormatError(Exception):
    def __init__(self, url: str, server_type: str):
        super().__init__(f"data received from {url} could not fulfill expected server type ({server_type})")


class NetworkInterfaces:
    """
    NetworkInterfaces: a class to monitor your interfaces
    """
    # make _interfaces a singleton since they can be shared on one system
    _instance: Optional["NetworkInterfaces"] = None
    _interfaces: Dict[str, NetworkInterface] = {}

    # A regular expression which separates the interesting fields and saves them in named groups
    # @see https://stackoverflow.com/questions/24897608/how-to-find-network-usage-in-linux-programatically
    _pattern = re.compile(r"\s*"
                          r"(?P<interface>\w+):\s+"
                          r"(?P<rx_bytes>\d+)\s+"
                          r"(?P<rx_packets>\d+)\s+"
                          r"(?P<rx_errs>\d+)\s+"
                          r"(?P<rx_drop>\d+)\s+"
                          r"(?P<rx_fifo>\d+)\s+"
                          r"(?P<rx_frame>\d+)\s+"
                          r"(?P<rx_compr>\d+)\s+"
                          r"(?P<rx_multicast>\d+)\s+"
                          r"(?P<tx_bytes>\d+)\s+"
                          r"(?P<tx_packets>\d+)\s+"
                          r"(?P<tx_errs>\d+)\s+"
                          r"(?P<tx_drop>\d+)\s+"
                          r"(?P<tx_fifo>\d+)\s+"
                          r"(?P<tx_frame>\d+)\s+"
                          r"(?P<tx_compr>\d+)\s+"
                          r"(?P<tx_multicast>\d+)\s*")

    _html_pattern = re.compile(r"\s*<(!DOCTYPE|html).*>.*")

    def __init__(self, interval: int = 10):
        self.interval = interval
        self._last_time_network_proc: float = 0.0
        self._do_fetch = False
        self._fetch_task: Optional[asyncio.Task] = None
        self._server_status: Optional[StubStatus] = None

    @staticmethod
    def get_instance() -> "NetworkInterfaces":
        if NetworkInterfaces._instance is None:
            NetworkInterfaces._instance = NetworkInterfaces()
        return NetworkInterfaces._instance

    @property
    def is_fetching(self):
        return self._do_fetch and self._fetch_task is not None

    @property
    def is_running(self):
        return self.is_fetching

    def get_server_status(self, attribute: str = "writing") -> Optional[int]:
        if self._server_status is None:
            return None
        return getattr(self._server_status, attribute, 0)

    def get_server_type(self):
        if self._server_status is None:
            raise KeyError("server type was never fetched")
        return getattr(self._server_status, "server_type", "Unknown")

    def get_interface_names(self) -> List[str]:
        return list(self._interfaces.keys())

    async def _fetch_proc_info(self):
        """Fetching data from /proc/net/dev"""
        with open('/proc/net/dev', 'r') as f:
            cur_time = time()
            a = f.readline()
            while a:
                match = self._pattern.search(a)
                # the regexp matched
                # look for the needed interface and return the rx_bytes and tx_bytes
                if match:
                    name = match.group("interface")
                    if name != "lo":
                        if name not in self._interfaces:
                            self._interfaces[name] = NetworkInterface(name)
                        for attr in ["packets", "drop", "errs", "fifo", "frame", "compr", "multicast"]:
                            self._set_interface_attr(name, f"rx_{attr}", int(match.group(f"rx_{attr}")))
                            self._set_interface_attr(name, f"tx_{attr}", int(match.group(f"tx_{attr}")))
                        for cls in ["tx", "rx"]:
                            attr = f"{cls}_bytes"
                            last_bytes = self.get_interface_attr(name, attr)
                            cur_bytes = int(match.group(attr))
                            interval = cur_time - self._last_time_network_proc
                            throughput = (cur_bytes - last_bytes) / interval
                            self._set_interface_attr(name, attr, cur_bytes)
                            self._set_interface_attr(name, f"{cls}_throughput", throughput)
                a = f.readline()

        self._last_time_network_proc = cur_time

    async def _fetch_server_status(self, url: str, logger: Optional[Logger]):
        if not url:
            return
        status: int
        ret: bytes
        try:
            status, ret = await HTTPClient.videbo_request("GET", url, external=True)
        except HTTPResponseError:
            if logger:
                logger.warning(f"error while handling internal request")
            return
        except ConnectionRefusedError:
            if logger:
                logger.warning(f"could not connect to {url}")
            return
        if status == 200:
            text = ret.decode("utf-8")
            lines = text.split("\n")
            if re.match(self._html_pattern, text):
                # Apache2 server status
                scan = False
                text_to_scan = ""
                updated = False
                for line in lines:
                    if re.match(r".*<pre>[A-Z_.]+", line):
                        scan = True
                    if scan:
                        text_to_scan += line
                    if scan and re.match(r"[A-Z_.]*</pre>.*", line):
                        # scan for first <pre> only
                        if self._server_status is None:
                            self._server_status = StubStatus()
                        self._server_status.writing = text_to_scan.count("W") - 1
                        self._server_status.reading = text_to_scan.count("R")
                        self._server_status.waiting = text_to_scan.count("_")
                        self._server_status.server_type = "Apache2"
                        updated = True
                        break
                if not updated:
                    err = UnknownServerStatusFormatError(url, "apache2")
                    if logger:
                        logger.warning(err.__str__())
                    raise err
            else:
                # nginx stub status
                try:
                    match = re.match(r"Reading:\s*(?P<reading>\d+)\s+"
                                     r"Writing:\s*(?P<writing>\d+)\s+"
                                     r"Waiting:\s*(?P<waiting>\d+)\s*", lines[3])
                except KeyError:
                    if logger:
                        logger.error(f"response to {url} has less than 4 lines: unexpected nginx stub status format")
                    raise UnknownServerStatusFormatError(url, "nginx")
                if match:
                    self._server_status = StubStatus()
                    self._server_status.writing = int(match.group("writing")) - 1
                    self._server_status.reading = int(match.group("reading"))
                    self._server_status.waiting = int(match.group("waiting"))
                    self._server_status.server_type = "nginx"
                else:
                    err = UnknownServerStatusFormatError(url, "nginx")
                    if logger:
                        logger.error(err.__str__())
                    raise err
        else:
            if logger:
                logger.warning(f"unexpected response from {url} (return code {status})")

    async def stop_fetching(self):
        """Stops the fetching process"""
        if self._fetch_task is not None:
            try:
                await self._fetch_task
            finally:
                self._do_fetch = False
                self._fetch_task = None
        else:
            self._do_fetch = False

    def start_fetching(self, status_page: Optional[str] = None, logger: Optional[Logger] = None):
        """Starts the fetching process"""
        if self._fetch_task is None:
            self._do_fetch = True

            async def fetcher():
                try:
                    # fetch in advance to avoid high throughput values
                    if logger:
                        logger.info("start fetch for network resources")
                    await self._fetch_proc_info()

                    while self._do_fetch:
                        try:
                            await self._fetch_proc_info()
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.exception(f"{e} in network _fetch_proc_info")

                        try:
                            await self._fetch_server_status(url=status_page, logger=logger)
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.exception(f"{e} in network _fetch_server_status")

                        await asyncio.sleep(self.interval)
                finally:
                    if logger:
                        logger.info("fetch for network resource information stopped")
                    self._fetch_task = None
                    self._do_fetch = False
            self._fetch_task = asyncio.create_task(fetcher())
            TaskManager.fire_and_forget_task(self._fetch_task)

    def get_interface(self, interface: str) -> Optional[NetworkInterface]:
        return self._interfaces.get(interface)

    def _set_interface_attr(self, interface: str, attr: str, value: Union[int, float]):
        """Sets only if interface exists"""
        if interface in self._interfaces:
            setattr(self._interfaces[interface], attr, value)

    def get_interface_attr(self, interface: str, attr: str) -> Optional[int]:
        """Returns the value if interface and attribute exist, else None"""
        if interface in self._interfaces:
            return getattr(self._interfaces[interface], attr, None)

    def update_node_status(self, status_obj: NodeStatus, server_status_page: str, logger: Logger) -> None:
        """Updates a given `NodeStatus` (subclass) instance with network interface information"""
        interfaces = self.get_interface_names()
        if server_status_page:
            status_obj.current_connections = self.get_server_status()
        if len(interfaces) > 0:
            # Just take the first network interface.
            interface = self.get_interface(interfaces[0])
            status_obj.tx_current_rate = int(interface.tx_throughput * 8 / 1_000_000)
            status_obj.rx_current_rate = int(interface.rx_throughput * 8 / 1_000_000)
            status_obj.tx_total = int(interface.tx_bytes / MEGA)
            status_obj.rx_total = int(interface.rx_bytes / MEGA)
        else:
            status_obj.tx_current_rate = 0
            status_obj.rx_current_rate = 0
            status_obj.tx_total = 0
            status_obj.rx_total = 0
            logger.error("No network interface found!")
