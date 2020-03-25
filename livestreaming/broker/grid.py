from livestreaming.broker.exceptions import *
from livestreaming.broker.api.models import *
from typing import Optional
from time import time


class BrokerGrid:

    _model: BrokerGridModel
    queue: BrokerQueue = []

    def __init__(self, model: Optional[BrokerGridModel] = None):
        if model is None:
            model = BrokerGridModel.empty_model()
        self._model = model

    def update(self, model: BrokerGridModel) -> None:
        self._model = model

    def get_stream(self, stream_id: int) -> BrokerStreams:
        return self._model.streams.get(stream_id)

    def get_streams(self) -> BrokerStreamCollection:
        return self._model.streams

    def get_content_node(self, host: str) -> BrokerContentNode:
        return self._model.content_nodes.get(host)

    def get_content_nodes(self) -> BrokerContentNodeCollection:
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

    def add_to_wait_queue(self, stream: int):
        time_current = time()
        time_delta = time_current - 60
        self.queue.append((stream, time_current))
        while True:
            if len(self.queue) == 0:
                break
            # else
            if time_delta > self.queue[0][1]:
                self.queue.pop()
            else:
                # time-series data: if first item of list does not match criteria, none else will
                break

    def json_model(self):
        queue_sum = {}
        for queue_item in self.queue:
            try:
                current = queue_sum[queue_item[0]]
            except KeyError:
                current = 0
            queue_sum[queue_item[0]] = current + 1
        return BrokerStateReturnData(streams=self.get_streams(),
                                     content_nodes=self.get_content_nodes(),
                                     queue=queue_sum)
