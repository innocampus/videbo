from typing import Optional, Dict
from livestreaming.web import JSONBaseModel
from livestreaming.web import BaseJWTData


class StartStreamDistributionInfo(JSONBaseModel):
    stream_id: int
    encoder_base_url: str
    broker_base_url: str
    max_clients: int  # -1 if no limit


class ContentStatus(JSONBaseModel):
    max_clients: int
    current_clients: int
    streams: Dict[int, int]  # map stream id to viewers counter


class StreamsMaxClients(JSONBaseModel):
    max_clients: Dict[int, int]  # maps stream id to max clients
