from livestreaming.broker.exceptions import *
from livestreaming.broker.api.models import *
from typing import Union


class BrokerGrid:

    _model: BrokerGridModel

    def __init__(self, model: Union[BrokerGridModel, None] = None):
        if model is None:
            model = BrokerGridModel.empty_model()
        self._model = model

    def update(self, model: BrokerGridModel) -> None:
        self._model = model

    def get_stream(self, stream_id: int) -> BrokerStreamModel:
        return self._model.streams.get(stream_id)

    def get_streams(self) -> BrokerStreamsModel:
        return self._model.streams

    def get_content_node(self, host: str) -> BrokerContentNodeModel:
        return self._model.content_nodes.get(host)

    def get_content_nodes(self) -> BrokerContentNodesModel:
        return self._model.content_nodes

    def is_available(self, host: str) -> bool:
        node = self._model.content_nodes.get(host)
        return node is not None and node.clients < node.max_clients

    def increment_clients(self, host: str):
        node = self._model.content_nodes.get(host)
        if node:
            node.clients += 1

    def get_penalty_ratio(self, host: str):
        node = self._model.content_nodes.get(host)
        if node is None:
            raise NodeNotFoundException(host)
        return node.penalty * (node.clients / node.max_clients)

