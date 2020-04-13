import asyncio
import re
from time import time
from typing import Optional
from typing import Dict
from typing import List
from typing import Union


class NetworkInterface:
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

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        # TODO: make it pretty
        return f"{self.__dict__}"


class NetworkInterfaces:
    """
    NetworkInterfaces: a class to monitor your interfaces
    """
    _instance: Optional["NetworkInterfaces"] = None

    # make _interfaces a singleton since they can be shared on one system
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

    def __init__(self, interval: int = 2):
        self.interval = interval
        self._last_time = 0
        self._do_fetch = False
        self._fetch_task: Optional[asyncio.Task] = None

    @staticmethod
    def get_instance() -> "NetworkInterfaces":
        if NetworkInterfaces._instance is None:
            NetworkInterfaces._instance = NetworkInterfaces()
        return NetworkInterfaces._instance


    @property
    def is_fetching(self):
        return self._do_fetch and self._fetch_task is not None

    def get_interface_names(self) -> List[str]:
        return list(self._interfaces.keys())

    async def _fetch_proc_info(self):
        """Fetching data from /proc/net/dev"""
        self._last_time = time()
        with open('/proc/net/dev', 'r') as f:
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
                            throughput = (cur_bytes-last_bytes)/self.interval
                            self._set_interface_attr(name, attr, cur_bytes)
                            self._set_interface_attr(name, f"{cls}_throughput", throughput)
                a = f.readline()

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

    def start_fetching(self):
        """Starts the fetching process"""
        if self._fetch_task is None:
            self._do_fetch = True

            async def fetcher():
                try:
                    # fetch in advance to avoid high throughput values
                    await self._fetch_proc_info()
                    while self._do_fetch:
                        await self._fetch_proc_info()
                        await asyncio.sleep(self.interval)
                finally:
                    self._fetch_task = None
                    self._do_fetch = False
            self._fetch_task = asyncio.create_task(fetcher())

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
