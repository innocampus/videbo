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
    STREAMING = 2 # streamer connected and streaming started
    STOPPED = 3
    ERROR = 100
    UNKNOWN = 200
