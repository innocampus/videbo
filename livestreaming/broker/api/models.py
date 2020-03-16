from typing import List, Dict
from livestreaming.web import JSONBaseModel
from livestreaming.web import BaseJWTData


class BrokerContentNodeModel(JSONBaseModel):
    max_clients: int
    clients: int
    load: float
    host: str
    penalty: int


BrokerStreamModel = List[str]
BrokerStreamsModel = Dict[int, BrokerStreamModel]
BrokerContentNodesModel = Dict[str, BrokerContentNodeModel]


class BrokerGridModel(JSONBaseModel):
    streams: BrokerStreamsModel
    content_nodes: BrokerContentNodesModel

    @staticmethod
    def empty_model():
        return BrokerGridModel(streams={}, content_nodes={})


class BrokerRedirectJWTData(BaseJWTData):
    stream_id: int
