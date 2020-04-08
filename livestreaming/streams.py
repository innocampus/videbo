from typing import Optional, Dict, TypeVar, Generic, Union
from ipaddress import IPv4Network, IPv6Network, ip_network
from time import time
from logging import Logger
from enum import IntEnum


class StreamState(IntEnum):
    UNKNOWN = 0
    NOT_YET_STARTED = 1
    WAITING_FOR_CONNECTION = 2  # streamer did not yet connect to open port
    BUFFERING = 3  # streamer connected, but waiting now until there is enough data (first segments have been written)
    STREAMING = 4  # streaming started, content node may fetch data from encoder and clients may connect to content node
    STOPPED = 5  # streamer stopped broadcasting, wait until manager tells to clean up
    ERROR = 100
    UNEXPECTED_STREAM = 104
    NO_ENCODER_AVAILABLE = 105


class Stream:
    def __init__(self, stream_id: int, ip_range: Optional[str], use_rtmps: bool, logger: Optional[Logger] = None):
        self.stream_id: int = stream_id
        self.ip_range: Union[IPv6Network, IPv4Network, None] = None
        self.use_rtmps: bool = use_rtmps
        self.rtmp_stream_key: Optional[str] = None
        self.encoder_subdir_name: Optional[str] = None
        self._state: StreamState = StreamState.UNKNOWN
        self.state_last_update: float = 0
        self._logger: Optional[Logger] = logger
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
        self._logger.info(f"<stream {self.stream_id}>: state changed: {StreamState(value).name}")

    @property
    def is_ip_restricted(self) -> bool:
        return self.ip_range is not None

    @property
    def ip_range_str(self) -> Optional[str]:
        return str(self.ip_range) if self.is_ip_restricted else None


StreamType = TypeVar("StreamType", bound=Stream)


class StreamCollection(Generic[StreamType]):
    streams: Dict[int, StreamType]

    def __init__(self):
        self.streams = {}

    def get_stream_by_id(self, stream_id: int) -> StreamType:
        return self.streams[stream_id]
