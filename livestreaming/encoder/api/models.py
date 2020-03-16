from enum import Enum
from typing import Optional
from livestreaming.web import JSONBaseModel


class NewStreamCreated(JSONBaseModel):
    url: str
    username: str
    password: str


class NewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[NewStreamCreated]
    error: Optional[str]


class StreamState(Enum):
    NOT_YET_STARTED = 0
    WAITING_FOR_CONNECTION = 1 # streamer did not yet connect to open port
    BUFFERING = 2 # streamer connected, but waiting now until there is enough data (first segments have been written)
    STREAMING = 3 # streaming started, content node may fetch data from encoder and clients may connect to content node
    STOPPED = 4 # streamer stopped broadcasting, wait until manager tells to clean up
    ERROR = 100
    UNKNOWN = 200
