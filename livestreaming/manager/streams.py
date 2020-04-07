import asyncio
from contextlib import asynccontextmanager
from time import time
from typing import Optional, Union, Dict, Set, List
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.auth import BaseJWTData
from livestreaming.streams import Stream, StreamCollection
from livestreaming.encoder.api.models import NewStreamReturn, NewStreamParams
from livestreaming.content.api.models import StartStreamDistributionInfo
from livestreaming.broker.api.models import BrokerContentNode, BrokerGridModel, BrokerStreamCollection
from livestreaming.manager.api.models import StreamStatusFull, StreamStatus
from . import logger
from .node_controller import NodeController
from .node_types import EncoderNode, ContentNode


class ManagerStream(Stream):
    def __init__(self, _stream_collection: 'ManagerStreamCollection', stream_id: int, ip_range: Optional[str],
                 use_rtmps: bool, lms_stream_instance_id: int):
        super().__init__(stream_id, ip_range, use_rtmps, logger)
        self._stream_collection: ManagerStreamCollection = _stream_collection
        self.encoder_streamer_url: str = ''
        self.lms_stream_instance_id = lms_stream_instance_id
        self.streamer_connection_until: Optional[int] = None
        self.control_task: Optional[asyncio.Task] = None
        self.encoder_listening_started: asyncio.Event = asyncio.Event()
        self.viewers: int = 0  # current viewers

        # Currently waiting clients that were rejected by the broker and can't watch the stream because content nodes
        # don't have enough available client slots.
        self.waiting_clients: int = 0

        # nodes
        self.encoder: Optional[EncoderNode] = None
        self.contents: Set[ContentNode] = set()

    def start(self):
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        """Controls the lifecycle of a stream."""
        async with self._stream_collection.hold_encoder_stream_slot() as encoder:
            self.encoder = encoder
            self.encoder_listening_started.set()
            await asyncio.sleep(900) # TODO

    async def tell_encoder_start(self, encoder: EncoderNode):
        """Tell encoder to start listening to an incoming stream."""
        url = f"{encoder.base_url}/api/encoder/stream/new/{self.stream_id}"
        stream_params = NewStreamParams(ip_range=self.ip_range_str, rtmps=self.use_rtmps,
                                        lms_stream_instance_id=self.lms_stream_instance_id)

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request_manager('POST', url, stream_params, NewStreamReturn)
            if ret.success:
                self.encoder_streamer_url = ret.stream.rtmp_public_url
                self.rtmp_stream_key = ret.stream.rtmp_stream_key
                self.encoder_subdir_name = ret.stream.encoder_subdir_name
                logger.info(f"Encoder: new <stream {self.stream_id}> encoder streamer url {self.encoder_streamer_url}")
            else:
                logger.error(f"Encoder: Could not create a new <stream {self.stream_id}>: {ret.error}")
                raise EncoderCreateNewStreamError()
        except HTTPResponseError as error:
            logger.exception(f"Encoder: Could not create a new <stream {self.stream_id}>, http error")
            raise EncoderCreateNewStreamError()

    async def tell_encoder_destroy(self):
        if self.encoder is None:
            return

        try:
            url = f"{self.encoder.base_url}/api/encoder/stream/{self.stream_id}/destroy"
            status, ret = await HTTPClient.internal_request_manager('GET', url)
            if status == 200:
                logger.info(f"Encoder: destroyed <stream {self.stream_id}>")
            else:
                logger.error(f"Encoder: Could not destroy <stream {self.stream_id}>: http status {status}")
        except HTTPResponseError as error:
            logger.exception(f"Encoder: Could not destroy <stream {self.stream_id}>, http error")
            # TODO retry

        self.encoder = None

    async def tell_content(self):
        jwt_data = BaseJWTData.construct(role='manager')
        url = f'http://localhost:9020/api/content/stream/start/{self.stream_id}'
        encoder_url = f'http://localhost:9010/data/hls/{self.stream_id}/{self.encoder_subdir_name}'
        info = StartStreamDistributionInfo(stream_id=self.stream_id, encoder_base_url=encoder_url)

        try:
            status, ret = await HTTPClient.internal_request('POST', url, jwt_data, info)
            if status != 200:
                raise ContentStartStreamingError()

        except HTTPResponseError as error:
            raise ContentStartStreamingError()

    def get_status(self, full: bool) -> Union[StreamStatus, StreamStatusFull]:
        data = {
            'stream_id': self.stream_id,
            'lms_stream_instance_id': self.lms_stream_instance_id,
            'state': self.state,
            'state_last_update': self.state_last_update,
            'viewers': 0,  # TODO
            'thumbnail_urls': []  # TODO
        }

        if full:
            data['streamer_url'] = self.encoder_streamer_url
            data['streamer_key'] = self.rtmp_stream_key
            data['streamer_ip_restricted'] = self.is_ip_restricted
            if self.streamer_connection_until:
                data['streamer_connection_time_left'] = self.streamer_connection_until - time()
            data['viewer_broker_url'] = self._stream_collection.get_broker_stream_main_playlist_url(self.stream_id)
            return StreamStatusFull(**data)
        else:
            return StreamStatus(**data)


class ManagerStreamCollection(StreamCollection[ManagerStream]):
    def __init__(self):
        super().__init__()
        self.last_stream_id: int = 1
        self.node_controller: Optional[NodeController] = None
        self._streams_control_tasks: Dict[asyncio.Task, ManagerStream] = {}
        self._new_stream_added_event: asyncio.Event = asyncio.Event()
        self._watcher_task: Optional[asyncio.Task] = None
        self._algorithm_task: Optional[asyncio.Task] = None
        self._encoder_nodes_controller: EncoderNodesController = EncoderNodesController()

    def init(self, node_controller: NodeController):
        self.node_controller = node_controller
        self._watcher_task = asyncio.create_task(self._watcher())
        self._algorithm_task = asyncio.create_task(self._algorithm_task_loop())
        self._encoder_nodes_controller.init(node_controller)

    async def _watcher(self):
        while True:
            new_stream_added_event_task = asyncio.create_task(self._new_stream_added_event.wait())
            tasks = {new_stream_added_event_task}
            tasks = tasks | self._streams_control_tasks.keys()
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            task: asyncio.Task
            for task in done:
                if task is not new_stream_added_event_task:
                    stream = self._streams_control_tasks.pop(task)
                    stream.encoder = None
                    stream.contents.clear()
                    self.streams.pop(stream)
                    try:
                        # Just call this to get the exception if there was any.
                        task.result()
                    except Exception:
                        logger.exception(f"Exception in stream control task for <stream {stream.stream_id}")
                    logger.info(f"Removed stream control task for <stream {stream.stream_id}")

            self._new_stream_added_event.clear()
            new_stream_added_event_task.cancel()

        # TODO way to cancel all control tasks

    def create_new_stream(self, ip_range: Optional[str], use_rtmps: bool, lms_stream_instance_id: int) -> ManagerStream:
        self.last_stream_id += 1
        new_stream = ManagerStream(self, self.last_stream_id, ip_range, use_rtmps, lms_stream_instance_id)
        new_stream.start()
        self.streams[new_stream.stream_id] = new_stream
        self._streams_control_tasks[new_stream.control_task] = new_stream
        self._new_stream_added_event.set()
        return new_stream

    @asynccontextmanager
    async def hold_encoder_stream_slot(self, stream: ManagerStream):
        encoder = await self._encoder_nodes_controller.find_encoder()
        try:
            await stream.tell_encoder_start(encoder)
            encoder.streams.add(stream)
            yield encoder

        finally:
            encoder.streams.remove(stream)
            await stream.tell_encoder_destroy()
            encoder.current_streams -= 1

    def get_broker_stream_main_playlist_url(self, stream_id: int) -> str:
        if self.node_controller.broker_node:
            return f"{self.node_controller.broker_node.base_url}/api/broker/redirect/{stream_id}.m3u8"
        raise BrokerNotFound()

    async def _algorithm_task_loop(self):

        await asyncio.sleep(10)


    async def tell_broker(self):
        jwt_data = BaseJWTData.construct(role='manager')
        url = f'http://localhost:9040/api/broker/streams'

        content_node = BrokerContentNode(clients=0, max_clients=1000, load=0, host='localhost:9020', penalty=1)
        contents = {
            f'localhost:9020': content_node
        }

        streams: BrokerStreamCollection = {
            2: ['localhost:9020']
        }

        grid = BrokerGridModel(streams=streams, content_nodes=contents)

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request('POST', url, jwt_data, grid, None)
            if status == 200:
                logger.info(f"Broker ok")
            else:
                logger.error(f"Broker error")
                raise CouldNotContactBrokerError()
        except HTTPResponseError as error:
            logger.error(f"Could not create a new stream: {error}")
            raise CouldNotContactBrokerError()


class EncoderNodesController:
    """Decides which new streams will be handled by which encoder nodes. It also manages the lifecycle of the
    dynamic encoders and may ask for more dynamic encoder nodes."""

    def __init__(self):
        self.node_controller: Optional[NodeController] = None
        self.encoders: List[EncoderNode] = []  # new nodes always to come to the end
        self.empty_since: Dict[EncoderNode, int]  # since when does a dynamic encoder node have no streams
        self.get_encoder_lock: asyncio.Lock = asyncio.Lock()

    def init(self, node_controller: NodeController):
        self.node_controller = node_controller
        nodes = self.node_controller.get_operating_nodes(EncoderNode)
        for node in nodes:
            self.encoders.append(node)

    async def find_encoder(self) -> EncoderNode:
        """Find an encoder that can have one more stream. Increases the internal current streams counter.
        Caller must decrease the counter by itself!"""
        if self.node_controller is None:
            logger.critical("EncoderNodesController: no node_controller set")
            raise EncoderNoStreamSlotAvailable()

        async with self.get_encoder_lock:
            # Find an encoder node that can have one more stream.
            encoder: Optional[EncoderNode] = None
            for node in self.encoders:
                if node.current_streams and node.current_streams < node.max_streams:
                    encoder = node

            if encoder is None:
                raise EncoderNoStreamSlotAvailable()
                # TODO request more encoder nodes!

            encoder.current_streams += 1
            return encoder


stream_collection = ManagerStreamCollection()


class EncoderCreateNewStreamError(Exception):
    pass


class EncoderNoStreamSlotAvailable(EncoderCreateNewStreamError):
    pass


class ContentStartStreamingError(Exception):
    pass


class CouldNotContactBrokerError(Exception):
    pass


class BrokerNotFound(Exception):
    pass