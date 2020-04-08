from typing import List, Dict, Tuple
from livestreaming.broker import broker_logger
from livestreaming.web import JSONBaseModel
from livestreaming.web import BaseJWTData


class BrokerContentNode(JSONBaseModel):
    max_clients: int
    clients: int
    base_url: str
    _penalty: int

    @property
    def penalty(self) -> int:
        return self._penalty

    @penalty.setter
    def penalty(self, value):
        if value < 0:
            msg = f"Penalty must be non-negative: got {value} for base_url <{self.base_url}>; penalty set to 0"
            broker_logger.warning(msg)
            self._penalty = 0
        else:
            self._penalty = value
        if self._penalty == 0:
            msg = f"Got penalty 0 for base_url <{self.base_url}>; <{self.base_url}> will always be prioritized"
            broker_logger.warning(msg)


BrokerStreamContents = List[str]
BrokerStreamCollection = Dict[int, BrokerStreamContents]
BrokerContentNodeCollection = Dict[str, BrokerContentNode]
BrokerQueue = List[Tuple[int, float]]  # list of tuples (stream_id, time)


class BrokerStateReturnData(JSONBaseModel):
    streams: BrokerStreamCollection
    content_nodes: BrokerContentNodeCollection
    queue: Dict[int, int]
    "stream_id -> number of waiting clients"


class BrokerGridModel(JSONBaseModel):
    streams: BrokerStreamCollection
    content_nodes: BrokerContentNodeCollection

    @staticmethod
    def empty_model():
        return BrokerGridModel(streams={}, content_nodes={})


class BrokerRedirectJWTData(BaseJWTData):
    stream_id: int
