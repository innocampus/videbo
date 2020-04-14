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

    def get_stream_nodes(self, stream_id: int) -> Optional[List[BrokerStreamContentNode]]:
        return self._model.streams.get(stream_id)

    def get_all_stream_nodes(self) -> BrokerStreamContentNodeCollection:
        return self._model.streams

    def get_content_node(self, node_id: int) -> BrokerContentNode:
        return self._model.content_nodes.get(node_id)

    def get_content_nodes(self) -> BrokerContentNodeCollection:
        return self._model.content_nodes

    def is_available(self, node_id: int) -> bool:
        node: BrokerStreamContentNode = self._model.streams.get(node_id)
        return node is not None and node.current_viewers < node.max_viewers

    def increment_clients(self, node_id: int):
        node = self._model.content_nodes.get(node_id)
        if node:
            node.clients += 1

    def get_penalty_ratio(self, node_id: int):
        node = self._model.content_nodes.get(node_id)
        if node is None:
            raise NodeNotFoundException(node_id)
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
        return BrokerStateReturnData(streams=self.get_all_stream_nodes(),
                                     content_nodes=self.get_content_nodes(),
                                     queue=queue_sum)
