from enum import Enum
from typing import Optional, List
from livestreaming.streams import StreamState
from livestreaming.web import JSONBaseModel


class NewStreamParams(JSONBaseModel):
    ip_range: Optional[str]


class NewStreamCreated(JSONBaseModel):
    url: str


class NewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[NewStreamCreated]
    error: Optional[str]


class EncoderStreamStatus(JSONBaseModel):
    stream_id: int
    state: StreamState
    state_last_update: int


class EncoderStatus(JSONBaseModel):
    max_streams: int
    current_streams: int
    streams: List[EncoderStreamStatus]
