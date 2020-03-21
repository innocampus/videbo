from enum import Enum
from typing import Optional
from livestreaming.web import JSONBaseModel


class NewStreamParams(JSONBaseModel):
    ip_range: Optional[str]


class NewStreamCreated(JSONBaseModel):
    url: str


class NewStreamReturn(JSONBaseModel):
    success: bool
    stream: Optional[NewStreamCreated]
    error: Optional[str]
