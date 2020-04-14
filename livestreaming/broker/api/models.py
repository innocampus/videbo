from typing import List
from typing import Dict
from typing import Tuple
from livestreaming.broker import broker_logger
from livestreaming.web import JSONBaseModel
from livestreaming.web import BaseJWTData


class BrokerContentNode(JSONBaseModel):
    max_clients: int
    clients: int
    base_url: str
    penalty: int

    """
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
    """


class BrokerStreamContentNode(JSONBaseModel):
    _stream_id: int
    node_id: int
    current_viewers: int
    max_viewers: int

    @property
    def stream_id(self) -> int:
        return self._stream_id


# stream id → content nodes to use with resp. number of viewers
BrokerStreamContentNodeCollection = Dict[int, List[BrokerStreamContentNode]]
# content node id → content node
BrokerContentNodeCollection = Dict[int, BrokerContentNode]
BrokerQueue = List[Tuple[int, float]]  # list of tuples (stream_id, time)


class BrokerStateReturnData(JSONBaseModel):
    streams: BrokerStreamContentNodeCollection
    content_nodes: BrokerContentNodeCollection
    queue: Dict[int, int]
    "stream_id -> number of waiting clients"


class BrokerGridModel(JSONBaseModel):
    streams: BrokerStreamContentNodeCollection
    content_nodes: BrokerContentNodeCollection

    @staticmethod
    def empty_model():
        return BrokerGridModel(streams={}, content_nodes={})


class BrokerRedirectJWTData(BaseJWTData):
    stream_id: int
    rid: str  # string to identify a unique user
