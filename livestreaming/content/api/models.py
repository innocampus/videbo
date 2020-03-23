from typing import Optional
from livestreaming.web import JSONBaseModel
from livestreaming.web import BaseJWTData


class ContentPlaylistJWTData(BaseJWTData):
    stream_id: int


class StartStreamDistributionInfo(JSONBaseModel):
    stream_id: int
    encoder_base_url: str
