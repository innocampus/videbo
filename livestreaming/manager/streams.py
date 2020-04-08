import asyncio
from contextlib import asynccontextmanager
from time import time
from typing import Optional, Union, Dict, Set, List, TYPE_CHECKING
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.auth import BaseJWTData
from livestreaming.streams import Stream, StreamCollection, StreamState
from livestreaming.encoder.api.models import NewStreamReturn, NewStreamParams
from livestreaming.content.api.models import StartStreamDistributionInfo
from livestreaming.broker.api.models import BrokerContentNode, BrokerGridModel, BrokerStreamCollection,\
    BrokerContentNodeCollection, BrokerStreamContents
from livestreaming.manager.api.models import StreamStatusFull, StreamStatus
from . import logger, manager_settings
from .node_controller import NodeController
from .node_types import EncoderNode, ContentNode
if TYPE_CHECKING:
    from .algorithms import StreamToContentType


class StreamStateObserver:
    """Observes stream state changes and notifies everyone who wants to know this."""
    def __init__(self, stream: 'ManagerStream'):
        self.stream: 'ManagerStream' = stream
        self.watchers: List[asyncio.Queue] = []

    def new_watcher(self) -> asyncio.Queue:
        queue = asyncio.Queue()
        self.watchers.append(queue)
        return queue

    def remove_watcher(self, queue: asyncio.Queue):
        self.watchers.remove(queue)

    def state_changed(self, new_state: StreamState):
        for watcher in self.watchers:
            watcher.put_nowait(new_state)

    @staticmethod
    async def wait_until(queue: asyncio.Queue, state: StreamState, including_errors: bool = True,
                         timeout: Optional[float] = None) -> StreamState:
        while True:
            new_state = await asyncio.wait_for(queue.get(), timeout)
            if new_state == state or (including_errors and new_state >= StreamState.ERROR):
                return new_state


class ManagerStream(Stream):
    def __init__(self, _stream_collection: 'ManagerStreamCollection', stream_id: int, ip_range: Optional[str],
                 use_rtmps: bool, lms_stream_instance_id: int, expected_viewers: Optional[int]):
        super().__init__(stream_id, ip_range, use_rtmps, logger)
        self._stream_collection: ManagerStreamCollection = _stream_collection
        self.encoder_streamer_url: str = ''
        self.lms_stream_instance_id = lms_stream_instance_id
        self.streamer_connection_until: Optional[int] = None
        self.control_task: Optional[asyncio.Task] = None
        self.state_observer: StreamStateObserver = StreamStateObserver(self)
        self.viewers: int = 0  # current viewers
        self.expected_viewers: Optional[int] = expected_viewers

        # Currently waiting clients that were rejected by the broker and can't watch the stream because content nodes
        # don't have enough available client slots.
        self.waiting_clients: int = 0

        # nodes
        self.encoder: Optional[EncoderNode] = None
        self.contents: Set[ContentNode] = set()

    def get_estimated_viewers(self):
        return max(self.viewers, self.expected_viewers)

    def update_state(self, new_state: StreamState, last_update: int = time()):
        if new_state != self._state:
            logger.info(f"<stream {self.stream_id}>: state changed from {StreamState(self._state).name} to "
                        f"{StreamState(new_state).name}")
            self._state = new_state
            self.state_last_update = last_update
            self.state_observer.state_changed(new_state)

    def start(self):
        self.control_task = asyncio.create_task(self.control())

    async def control(self):
        """Controls the lifecycle of a stream."""
        logger.info(f"Start new <stream {self.stream_id}>")
        state_watcher = self.state_observer.new_watcher()
        try:
            async with self._stream_collection.hold_encoder_stream_slot(self) as encoder:
                self.encoder = encoder
                await StreamStateObserver.wait_until(state_watcher, StreamState.STOPPED)
        except EncoderNoStreamSlotAvailable:
            self.update_state(StreamState.NO_ENCODER_AVAILABLE)
        finally:
            for content in list(self.contents): # copy to list because we modify self.contents in loop
                await self.tell_content_destroy(content)
            logger.info(f"<stream {self.stream_id}> ended with status {StreamState(self.state).name}")

    async def tell_encoder_start(self, encoder: EncoderNode):
        """Tell encoder to start listening to an incoming stream."""
        url = f"{encoder.base_url}/api/encoder/stream/new/{self.stream_id}"
        stream_params = NewStreamParams(ip_range=self.ip_range_str, rtmps=self.use_rtmps,
                                        lms_stream_instance_id=self.lms_stream_instance_id)

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request_manager('POST', url, stream_params, NewStreamReturn)
            if ret.success:
                self.encoder_streamer_url = f"rtmp://{encoder.server.host}:{ret.stream.rtmp_port}/stream"
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

    async def tell_content_start(self, content: ContentNode):
        self.contents.add(content)
        content.streams.add(self)
        url = f"{content.base_url}/api/content/stream/start/{self.stream_id}"
        encoder_url = f"{self.encoder.base_url}/data/hls/{self.stream_id}/{self.encoder_subdir_name}"
        info = StartStreamDistributionInfo(stream_id=self.stream_id, encoder_base_url=encoder_url)

        try:
            status, ret = await HTTPClient.internal_request_manager('POST', url, info)
            if status == 200:
                logger.info(f"Content: new <stream {self.stream_id}> on {content.server.name}")
            else:
                logger.error(f"Content: Could not add <stream {self.stream_id}> on {content.server.name}: "
                             f"got http status {status}")
                raise ContentStartStreamingError()

        except HTTPResponseError as error:
            logger.exception(f"Content: Could not add <stream {self.stream_id}> on {content.server.name}, http error")
            raise ContentStartStreamingError()

    async def tell_content_destroy(self, content: ContentNode):
        content.streams.remove(self)
        self.contents.remove(content)
        try:
            url = f"{content.base_url}/api/content/stream/{self.stream_id}/destroy"
            status, ret = await HTTPClient.internal_request_manager('GET', url)
            if status == 200:
                logger.info(f"Content: destroyed <stream {self.stream_id}>")
            else:
                logger.error(f"Content: Could not destroy <stream {self.stream_id}>: http status {status}")
        except HTTPResponseError as error:
            logger.exception(f"Content: Could not destroy <stream {self.stream_id}>, http error")
            # TODO retry

    def get_status(self, full: bool) -> Union[StreamStatus, StreamStatusFull]:
        data = {
            'stream_id': self.stream_id,
            'lms_stream_instance_id': self.lms_stream_instance_id,
            'state': self.state,
            'state_last_update': self.state_last_update,
            'viewers': self.viewers,
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
            tasks = [new_stream_added_event_task]
            tasks = tasks | self._streams_control_tasks.keys()
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            task: asyncio.Task
            for task in done:
                if task is not new_stream_added_event_task:
                    stream = self._streams_control_tasks.pop(task)
                    stream.encoder = None
                    stream.contents.clear()
                    self.streams.pop(stream.stream_id)
                    try:
                        # Just call this to get the exception if there was any.
                        task.result()
                    except Exception:
                        logger.exception(f"Exception in stream control task for <stream {stream.stream_id}>")
                    logger.info(f"Removed stream control task for <stream {stream.stream_id}>")

            self._new_stream_added_event.clear()
            new_stream_added_event_task.cancel()

        # TODO way to cancel all control tasks

    def create_new_stream(self, ip_range: Optional[str], use_rtmps: bool, lms_stream_instance_id: int,
                          expected_viewers: Optional[int]) -> ManagerStream:
        self.last_stream_id += 1
        new_stream = ManagerStream(self, self.last_stream_id, ip_range, use_rtmps, lms_stream_instance_id,
                                   expected_viewers)
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
            async with encoder.streams_lock:
                encoder.streams.add(stream)
            yield encoder

        finally:
            async with encoder.streams_lock:
                encoder.streams.remove(stream)
            await stream.tell_encoder_destroy()
            encoder.current_streams -= 1

    def get_broker_stream_main_playlist_url(self, stream_id: int) -> str:
        if self.node_controller.broker_node:
            return f"{self.node_controller.broker_node.base_url}/api/broker/redirect/{stream_id}/main.m3u8"
        raise BrokerNotFound()

    async def _algorithm_task_loop(self):
        from .algorithms import get_algorithm
        try:
            algorithm = get_algorithm(manager_settings.streams_content_node_distribution_algorithm)
            while True:
                contents = self.node_controller.get_operating_nodes(ContentNode)
                ret = await algorithm.solve(list(self.streams.values()), contents)
                # TODO ret.clients_left_out
                if ret.stream_to_content:
                    await self._compute_diff_content_nodes_and_inform(ret.stream_to_content)

                    try:
                        await self._tell_broker(contents)
                    except CouldNotContactBrokerError:
                        pass

                await asyncio.sleep(2)
        except:
            logger.exception("Algorithm task loop exception")
            raise

    async def _compute_diff_content_nodes_and_inform(self, dist: "StreamToContentType"):
        awaitables = []
        for stream_id, new_content_ids in dist:
            stream = self.streams[stream_id]
            new_contents: Set[ContentNode] = set()
            for new_content_id in new_content_ids:
                node = self.node_controller.node_by_id[new_content_id]
                if isinstance(node, ContentNode):
                    new_contents.add(node)
                else:
                    logger.error(f"Algorithm task: Got unexpected node type {type(node).__name__} when expected "
                                 f"ContentNode")

            if stream.state != StreamState.STREAMING:
                continue

            # all content nodes that should no longer carry this stream
            for rem_content in (stream.contents - new_contents):
                awaitables.append(stream.tell_content_destroy(rem_content))
            # all content nodes that should start to carry this stream
            for new_content in (new_contents - stream.contents):
                awaitables.append(stream.tell_content_start(new_content))

        await asyncio.gather(*awaitables) # TODO handle exceptions

    async def _tell_broker(self, content_list: List[ContentNode]):
        # get all content nodes
        contents: BrokerContentNodeCollection = {}
        for content in content_list:
            contents[content.base_url] = BrokerContentNode(clients=content.current_clients,
                                                           max_clients=content.max_clients,
                                                           base_url=content.base_url,
                                                           penalty=1)

        # get all streams
        all_streams: BrokerStreamCollection = {}
        for stream in self.streams.values():
            if stream.state == StreamState.STREAMING:
                cnodes: BrokerStreamContents = []
                for content in stream.contents:
                    cnodes.append(content.base_url)
                all_streams[stream.stream_id] = cnodes

        grid = BrokerGridModel(streams=all_streams, content_nodes=contents)

        try:
            url = f'{self.node_controller.broker_node.base_url}/api/broker/streams'
            status, ret = await HTTPClient.internal_request_manager('POST', url, grid)
            if status == 200:
                logger.debug(f"Broker updated")
            else:
                logger.error(f"Broker error, http status {status}")
                raise CouldNotContactBrokerError()
        except HTTPResponseError as error:
            logger.exception(f"Broker error")
            raise CouldNotContactBrokerError()


class EncoderNodesController:
    """Decides which new streams will be handled by which encoder nodes. It also manages the
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
            # list in node_controller should be ordered by the time when a node was added
            for node in self.node_controller.get_operating_nodes(EncoderNode):
                if node.current_streams is not None and node.current_streams < node.max_streams:
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