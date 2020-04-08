from asyncio import Task, sleep, Lock
from typing import Optional, Dict, TYPE_CHECKING
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.streams import StreamState
from livestreaming.encoder.api.models import EncoderStatus, NewStreamParams, NewStreamReturn
from livestreaming.content.api.models import ContentStatus, StartStreamDistributionInfo
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

        self.streams: Dict[int, "ManagerStream"] = {}  # stream id to ManagerStream
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
                        for stream_id, stream in self.streams.items():
                            if stream_id in ret.streams:
                                ret_stream = ret.streams[stream_id]
                                stream.update_state(ret_stream.state, ret_stream.state_last_update)
                            else:
                                logger.error(f"<Encoder {self.server.name}> should have <stream {stream_id}>, "
                                             f"but did not found in status data.")

                        # Check if all nodes in ret.streams still exist in self.streams and their status is valid.
                        for stream_id in ret.streams.keys():
                            if stream_id not in self.streams or self.streams[stream_id].state >= StreamState.ERROR:
                                await self.destroy_stream(stream_id, True)

                    if first_request:
                        logger.info(f"<Encoder watcher {self.server.name}> max_streams={self.max_streams}, "
                                    f"current_streams={self.current_streams}")
                        first_request = False

            except HTTPResponseError:
                logger.exception(f"<Encoder watcher {self.server.name}> http error")
            await sleep(1)

    async def start_stream(self, stream: "ManagerStream") -> NewStreamReturn:
        """Tell encoder to start listening to an incoming stream."""
        async with self.streams_lock:
            self.streams[stream.stream_id] = stream

        url = f"{self.base_url}/api/encoder/stream/new/{stream.stream_id}"
        stream_params = NewStreamParams(ip_range=stream.ip_range_str, rtmps=stream.use_rtmps,
                                        lms_stream_instance_id=stream.lms_stream_instance_id)

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request_manager('POST', url, stream_params, NewStreamReturn)
            if ret.success:
                logger.info(f"Encoder: new <stream {stream.stream_id}> on <encoder {self.server.name}>")
                return ret
            else:
                logger.error(f"Encoder: Could not create a new <stream {stream.stream_id}>: {ret.error}")
                raise EncoderCreateNewStreamError()
        except HTTPResponseError as error:
            logger.exception(f"Encoder: Could not create a new <stream {stream.stream_id}>, http error")
            raise EncoderCreateNewStreamError()

    async def destroy_stream(self, stream_id: int, force_destroy: bool = False):
        async with self.streams_lock:
            stream = self.streams.pop(stream_id, None)
            if stream is None and not force_destroy:
                # Stream was already removed.
                return
            self.current_streams = len(self.streams)

        try:
            url = f"{self.base_url}/api/encoder/stream/{stream_id}/destroy"
            status, ret = await HTTPClient.internal_request_manager('GET', url)
            if status == 200:
                logger.info(f"Encoder: destroyed <stream {stream_id}>")
            else:
                logger.error(f"Encoder: Could not destroy <stream {stream_id}>: http status {status}")
        except HTTPResponseError as error:
            logger.exception(f"Encoder: Could not destroy <stream {stream_id}>, http error")

        # Ignore errors here. The watchdog will try to destroy the stream later again.


class ContentNode(NodeTypeBase):
    def __init__(self):
        super().__init__()
        self.max_clients: int = 0
        self.current_clients: int = 0
        self.streams: Dict[int, "ManagerStream"] = {}  # stream id to ManagerStream
        self.streams_lock: Lock = Lock()

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

                    async with self.streams_lock:
                        for stream_id, stream in self.streams.items():
                            if stream_id not in ret.streams:
                                logger.error(f"<Content {self.server.name}> should have <stream {stream_id}>, "
                                             f"but did not found in status data.")

                        # Check if all nodes in ret.streams still exist in self.streams and their status is valid.
                        for stream_id in ret.streams.keys():
                            if stream_id not in self.streams or self.streams[stream_id].state >= StreamState.ERROR:
                                await self.destroy_stream(stream_id, True)

                    if first_request:
                        logger.info(f"<Content watcher {self.server.name}> max_clients={self.max_clients}, "
                                    f"current_clients={self.current_clients}")
                        first_request = False

            except HTTPResponseError:
                logger.exception(f"<Content watcher {self.server.name}> http error")
            await sleep(1)

    async def start_stream(self, stream: "ManagerStream"):
        async with self.streams_lock:
            self.streams[stream.stream_id] = stream

        url = f"{self.base_url}/api/content/stream/start/{stream.stream_id}"
        encoder_url = f"{stream.encoder.base_url}/data/hls/{stream.stream_id}/{stream.encoder_subdir_name}"
        info = StartStreamDistributionInfo(stream_id=stream.stream_id, encoder_base_url=encoder_url)

        try:
            status, ret = await HTTPClient.internal_request_manager('POST', url, info)
            if status == 200:
                logger.info(f"Content: new <stream {stream.stream_id}> on {self.server.name}")
            else:
                logger.error(f"Content: Could not add <stream {stream.stream_id}> on {self.server.name}: "
                             f"got http status {status}")
                raise ContentStartStreamingError(self, stream)

        except HTTPResponseError as error:
            logger.exception(f"Content: Could not add <stream {stream.stream_id}> on {self.server.name}, http error")
            raise ContentStartStreamingError(self, stream)

    async def destroy_stream(self, stream_id: int, force_destroy: bool = False):
        async with self.streams_lock:
            stream = self.streams.pop(stream_id, None)
            if stream is None and not force_destroy:
                # Stream was already removed.
                return

        try:
            url = f"{self.base_url}/api/content/stream/{stream_id}/destroy"
            status, ret = await HTTPClient.internal_request_manager('GET', url)
            if status == 200:
                logger.info(f"Content: destroyed <stream {stream_id}>")
            else:
                logger.error(f"Content: Could not destroy <stream {stream_id}>: http status {status}")
        except HTTPResponseError as error:
            logger.exception(f"Content: Could not destroy <stream {stream_id}>, http error")

        # Ignore errors here. The watchdog will try to destroy the stream later again.


class BrokerNode(NodeTypeBase):
    async def watchdog(self): # TODO
        while True:
            await sleep(1)


class StorageNode(NodeTypeBase):
    async def watchdog(self): # TODO
        while True:
            await sleep(1)


#
# Exceptions
#

class EncoderCreateNewStreamError(Exception):
    pass


class ContentStartStreamingError(Exception):
    def __init__(self, content: ContentNode, stream: "ManagerStream"):
        self.content = content
        self.stream = stream
        super().__init__("Could not add stream to content node.")
