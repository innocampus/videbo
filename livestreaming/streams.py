from typing import Optional, Dict, TypeVar, Generic, Union
from ipaddress import IPv4Network, IPv6Network, ip_network
from time import time
from logging import Logger
from enum import Enum


class StreamState(Enum):
    NOT_YET_STARTED = 0
    WAITING_FOR_CONNECTION = 1 # streamer did not yet connect to open port
    BUFFERING = 2 # streamer connected, but waiting now until there is enough data (first segments have been written)
    STREAMING = 3 # streaming started, content node may fetch data from encoder and clients may connect to content node
    STOPPED = 4 # streamer stopped broadcasting, wait until manager tells to clean up
    ERROR = 100
    UNEXPECTED_STREAM = 104
    UNKNOWN = 200


class Stream:
    stream_id: int
    ip_range: Union[IPv6Network, IPv4Network, None] = None
    _state: StreamState = StreamState.UNKNOWN
    state_last_update: float = 0
    _logger: Optional[Logger] = None

    def __init__(self, stream_id: int, ip_range: Optional[str], logger: Optional[Logger] = None):
        self.stream_id = stream_id
        self._logger = logger
        try:
            self.ip_range = ip_network(ip_range)
        except ValueError as e:
            if self._logger and ip_range:
                self._logger.warning(e)
            self.ip_range = None

    @property
    def state(self) -> StreamState:
        return self._state

    @state.setter
    def state(self, value: StreamState):
        self._state = value
        self.state_last_update = time()

    @property
    def is_restricted(self) -> bool:
        return self.ip_range is not None

    @property
    def ip_range_str(self) -> Optional[str]:
        return str(self.ip_range) if self.is_restricted else None


StreamType = TypeVar("StreamType", bound=Stream)


class StreamCollection(Generic[StreamType]):
    streams: Dict[int, StreamType] = {}

    def get_stream_by_id(self, stream_id: int) -> StreamType:
        return self.streams[stream_id]
