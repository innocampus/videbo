from typing import Optional, List
from livestreaming.web import JSONBaseModel
from livestreaming.streams import StreamState


class LMSNewStreamParams(JSONBaseModel):
    ip_range: Optional[str]
    rtmps: bool
    lms_stream_instance_id: int


class StreamStatus(JSONBaseModel):
    stream_id: int
    lms_stream_instance_id: int
    state: StreamState
    state_last_update: int
    viewers: int
    thumbnail_urls: List[str]


class StreamStatusFull(StreamStatus):
    streamer_url: str
    streamer_key: str
    streamer_ip_restricted: bool
    streamer_connection_time_left: Optional[int]  # time left (in seconds) to connect to encoder
    viewer_broker_url: str


class LMSNewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[StreamStatusFull]
    error: Optional[str]


class AllStreamsStatus(JSONBaseModel):
    streams: List[StreamStatus]
