from typing import Optional
from livestreaming.web import JSONBaseModel


class LMSNewStreamCreated(JSONBaseModel):
    stream_id: int
    streamer_url: str
    streamer_username: str
    streamer_password: str
    viewer_broker_url: str


class LMSNewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[LMSNewStreamCreated]
    error: Optional[str]
