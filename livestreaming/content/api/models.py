from typing import Optional
from livestreaming.web import JSONBaseModel


class StartStreamDistributionInfo(JSONBaseModel):
    stream_id: int
    encoder_base_url: str
