from typing import Optional
from livestreaming.web import JSONBaseModel


class LMSNewStreamParams(JSONBaseModel):
    ip_range: Optional[str]


class LMSNewStreamCreated(JSONBaseModel):
    stream_id: int
    streamer_url: str
    viewer_broker_url: str
    ip_restricted: bool


class LMSNewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[LMSNewStreamCreated]
    error: Optional[str]
