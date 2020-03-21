from typing import Optional, Dict, TypeVar, Generic
from enum import Enum


class StreamState(Enum):
    NOT_YET_STARTED = 0
    WAITING_FOR_CONNECTION = 1 # streamer did not yet connect to open port
    BUFFERING = 2 # streamer connected, but waiting now until there is enough data (first segments have been written)
    STREAMING = 3 # streaming started, content node may fetch data from encoder and clients may connect to content node
    STOPPED = 4 # streamer stopped broadcasting, wait until manager tells to clean up
    ERROR = 100
    UNKNOWN = 200


class Stream:
    stream_id: int
    ip_range: Optional[str]
    state: StreamState = StreamState.UNKNOWN
    state_last_update: float = 0

    def __init__(self, stream_id: int, ip_range: Optional[str]):
        self.stream_id = stream_id
        self.ip_range = ip_range
        if not self._validate_ip_range():
            self.ip_range = None

    def _validate_ip_range(self) -> bool:
        return self.ip_range is not None   # TODO


StreamType = TypeVar("StreamType", bound=Stream)


class StreamCollection(Generic[StreamType]):
    streams: Dict[int, StreamType] = {}

    def get_stream_by_id(self, stream_id: int) -> StreamType:
        return self.streams[stream_id]
