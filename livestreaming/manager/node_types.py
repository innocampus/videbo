from asyncio import Task, sleep, Lock
from typing import Optional, Set, TYPE_CHECKING
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.encoder.api.models import EncoderStatus
from livestreaming.content.api.models import ContentStatus
from .cloud.server import Server
from . import logger
if TYPE_CHECKING:
    from .streams import ManagerStream


class NodeTypeBase:
    _last_id: int = 1

    def __init__(self):
        self.lifecycle_task: Optional[Task] = None
        self.server: Optional[Server] = None
        self.base_url: Optional[str] = None  # does not end with a slash
        self.id: int = NodeTypeBase._last_id  # internal id of the node
        NodeTypeBase._last_id += 1

    async def watchdog(self):
        raise NotImplementedError()

    def is_operational(self) -> bool:
        return self.server is not None and self.server.is_operational()


class EncoderNode(NodeTypeBase):
    def __init__(self):
        super().__init__()
        self.max_streams: int = 0

        # Current streams is updated by the manager. When None the number is taken from the encoder.
        # We want to avoid race conditions, but we also need to get an initial number when starting the manager.
        self.current_streams: Optional[int] = None

        self.streams: Set["ManagerStream"] = set()
        self.streams_lock: Lock = Lock()

    async def watchdog(self):
        status_url = f"{self.base_url}/api/encoder/status"
        first_request = True
        while True:
            try:
                ret: EncoderStatus
                status, ret = await HTTPClient.internal_request_manager('GET', status_url, None, EncoderStatus)
                if status == 200:
                    self.max_streams = ret.max_streams
                    if self.current_streams is None:
                        self.current_streams = ret.current_streams

                    async with self.streams_lock:
                        for stream in self.streams:
                            if stream.stream_id in ret.streams:
                                ret_stream = ret.streams[stream.stream_id]
                                stream.update_state(ret_stream.state, ret_stream.state_last_update)
                            else:
                                logger.error(f"<Encoder {self.server.name}> should have <stream {stream.stream_id}>, "
                                             f"but did not found in status data.")
                        # TODO check if all nodes in ret.streams still exist in self.streams and their status is valid

                    if first_request:
                        logger.info(f"<Encoder watcher {self.server.name}> max_streams={self.max_streams}, "
                                    f"current_streams={self.current_streams}")
                        first_request = False

            except HTTPResponseError:
                logger.exception(f"<Encoder watcher {self.server.name}> http error")
            await sleep(1)


class ContentNode(NodeTypeBase):
    def __init__(self):
        super().__init__()
        self.max_clients: int = 0
        self.current_clients: int = 0
        self.streams: Set["ManagerStream"] = set()

    async def watchdog(self):
        status_url = f"{self.base_url}/api/content/status"
        first_request = True
        while True:
            try:
                ret: ContentStatus
                status, ret = await HTTPClient.internal_request_manager('GET', status_url, None, ContentStatus)
                if status == 200:
                    self.max_clients = ret.max_clients
                    self.current_clients = ret.current_clients

                    if first_request:
                        logger.info(f"<Content watcher {self.server.name}> max_clients={self.max_clients}, "
                                    f"current_clients={self.current_clients}")
                        first_request = False

            except HTTPResponseError:
                logger.exception(f"<Content watcher {self.server.name}> http error")
            await sleep(1)


class BrokerNode(NodeTypeBase):
    async def watchdog(self): # TODO
        while True:
            await sleep(1)


class StorageNode(NodeTypeBase):
    async def watchdog(self): # TODO
        while True:
            await sleep(1)
