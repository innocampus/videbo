import asyncio
from contextlib import asynccontextmanager
from time import time
from typing import Optional, Union, Dict, Set, List, TYPE_CHECKING
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.streams import Stream, StreamCollection, StreamState
from livestreaming.broker.api.models import *
from livestreaming.manager.api.models import StreamStatusFull, StreamStatus
from livestreaming.misc import TaskManager
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
    async def wait_until(queue: asyncio.Queue, state: StreamState, or_greater_state: bool = True,
                         including_errors: bool = True, timeout: Optional[float] = None) -> StreamState:
        while True:
            new_state = await asyncio.wait_for(queue.get(), timeout)
            if new_state == state or (or_greater_state and new_state >= state) or \
                    (including_errors and new_state >= StreamState.ERROR):
                return new_state


class ContentNodeStream:
    def __init__(self, max_clients: int = -1):
        self.current_clients: int = 0
        self.max_clients: int = max_clients


class ManagerStream(Stream):
    MAX_WAIT_UNTIL_CONNECTION = 600
    ADDITIONAL_CLIENTS_PER_STREAM = 10  # always available client slots per stream

    def __init__(self, _stream_collection: 'ManagerStreamCollection', stream_id: int, ip_range: Optional[str],
                 use_rtmps: bool, lms_stream_instance_id: int, expected_viewers: Optional[int]):
        super().__init__(stream_id, ip_range, use_rtmps, logger)
        self.stream_collection: ManagerStreamCollection = _stream_collection
        self.lms_stream_instance_id = lms_stream_instance_id
        self.encoder_rtmp_port: Optional[int] = None
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
        self.contents: Dict[ContentNode, ContentNodeStream] = {}  # map content node to ContentNodeStream

    def get_estimated_viewers(self):
        current_estimated_users = self.viewers + self.ADDITIONAL_CLIENTS_PER_STREAM + self.waiting_clients
        if self.expected_viewers:
            return max(current_estimated_users, self.expected_viewers)
        else:
            return current_estimated_users

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
            async with self.stream_collection.hold_encoder_stream_slot(self) as encoder:
                self.encoder: EncoderNode = encoder
                ret = await self.encoder.start_stream(self)
                self.rtmp_stream_key = ret.stream.rtmp_stream_key
                self.encoder_rtmp_port = ret.stream.rtmp_port
                self.encoder_subdir_name = ret.stream.encoder_subdir_name
                self.streamer_connection_until = time() + self.MAX_WAIT_UNTIL_CONNECTION

                try:
                    await asyncio.wait_for(StreamStateObserver.wait_until(state_watcher, StreamState.BUFFERING),
                                           self.MAX_WAIT_UNTIL_CONNECTION)
                except asyncio.TimeoutError:
                    # streamer did not connect to RTMP server. Stop.
                    self.update_state(StreamState.STREAMER_DID_NOT_CONNECT)
                    return

                await StreamStateObserver.wait_until(state_watcher, StreamState.STOPPED)

                # Wait shortly until removing all content nodes
                await asyncio.sleep(2)
                self.contents.clear()  # ContentNode watchdog will remove the streams

                # encoder.destroy_stream will be called by the context manager

        except EncoderNoStreamSlotAvailable:
            self.update_state(StreamState.NO_ENCODER_AVAILABLE)
        except asyncio.CancelledError:
            self.update_state(StreamState.STOPPED_FORCEFULLY)
        except Exception as e:
            if self.state < StreamState.ERROR:
                self.update_state(StreamState.ERROR)
            raise e
        finally:
            logger.info(f"<stream {self.stream_id}> ended with status {self.state_name}")

    def stop(self):
        self.control_task.cancel()
        logger.info(f"Forcefully stop <stream {self.stream_id}>")

    def get_status(self, full: bool) -> Union[StreamStatus, StreamStatusFull]:
        prot = "rtmps" if self.use_rtmps else 'rtmp'
        encoder_streamer_url = f"{prot}://{self.encoder.server.host}:{self.encoder_rtmp_port}/stream"

        data = {
            'stream_id': self.stream_id,
            'lms_stream_instance_id': self.lms_stream_instance_id,
            'state': self.state,
            'state_last_update': self.state_last_update,
            'viewers': self.viewers,
            'thumbnail_urls': []  # TODO
        }

        if full:
            data['streamer_url'] = encoder_streamer_url
            data['streamer_key'] = self.rtmp_stream_key
            data['streamer_ip_restricted'] = self.is_ip_restricted
            if self.streamer_connection_until:
                data['streamer_connection_until'] = self.streamer_connection_until
            data['viewer_broker_url'] = self.stream_collection.get_broker_stream_main_playlist_url(self.stream_id)
            return StreamStatusFull(**data)
        else:
            return StreamStatus(**data)


class ManagerStreamCollection(StreamCollection[ManagerStream]):
    def __init__(self):
        super().__init__()
        self.last_stream_id: int = 1
        self.node_controller: Optional[NodeController] = None
        self._streams_control_tasks: Dict[asyncio.Task, ManagerStream] = {}
        self._algorithm_task: Optional[asyncio.Task] = None
        self._encoder_nodes_controller: EncoderNodesController = EncoderNodesController()

    def init(self, node_controller: NodeController):
        self.node_controller = node_controller
        self._algorithm_task = asyncio.create_task(self._algorithm_task_loop())
        TaskManager.fire_and_forget_task(self._algorithm_task)
        self._encoder_nodes_controller.init(node_controller)

    def create_new_stream(self, ip_range: Optional[str], use_rtmps: bool, lms_stream_instance_id: int,
                          expected_viewers: Optional[int]) -> ManagerStream:
        self.last_stream_id += 1
        new_stream = ManagerStream(self, self.last_stream_id, ip_range, use_rtmps, lms_stream_instance_id,
                                   expected_viewers)
        new_stream.start()

        def stream_control_task_done(future):
            new_stream.encoder = None
            new_stream.contents.clear()
            self.streams.pop(new_stream.stream_id)
            try:
                # Just call this to get the exception if there was any.
                future.result()
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception(f"Exception in stream control task for <stream {new_stream.stream_id}>")
            logger.info(f"Removed stream control task for <stream {new_stream.stream_id}>")

        self.streams[new_stream.stream_id] = new_stream
        new_stream.control_task.add_done_callback(stream_control_task_done)
        return new_stream

    @asynccontextmanager
    async def hold_encoder_stream_slot(self, stream: ManagerStream):
        encoder = await self._encoder_nodes_controller.find_encoder()
        try:
            yield encoder

        finally:
            await encoder.destroy_stream(stream.stream_id)

    def get_broker_stream_main_playlist_url(self, stream_id: int) -> str:
        if self.node_controller.broker_node:
            return f"{self.node_controller.broker_node.base_url}/api/broker/redirect/{stream_id}/main.m3u8"
        raise BrokerNotFound()

    def get_broker_base_url(self):
        if self.node_controller.broker_node:
            return self.node_controller.broker_node.base_url
        raise BrokerNotFound()

    async def _algorithm_task_loop(self):
        from .algorithms import get_algorithm

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

    async def _compute_diff_content_nodes_and_inform(self, dist: "StreamToContentType"):
        awaitables = []
        for stream, new_contents in dist.items():
            if stream.state != StreamState.STREAMING:
                continue

            # all content nodes that should no longer carry this stream
            for rem_content in [n for n in stream.contents.keys() if n not in new_contents]:
                stream.contents.pop(rem_content)
                awaitables.append(rem_content.destroy_stream(stream.stream_id))
            # all content nodes that should start to carry this stream
            for new_content, max_clients in \
                    [(n, max_clients) for n, max_clients in new_contents.items() if n not in stream.contents]:
                stream.contents[new_content] = ContentNodeStream(max_clients)
                awaitables.append(new_content.start_stream(stream))

        await asyncio.gather(*awaitables, return_exceptions=True)
        # Ignore exceptions. The content watchdog will try to add/remove the streams later again.

    async def _tell_broker(self, content_list: List[ContentNode]):
        # get all content nodes
        contents: BrokerContentNodeCollection = {}
        for content in content_list:
            contents[content.id] = BrokerContentNode(clients=content.current_clients,
                                                     max_clients=content.max_clients,
                                                     base_url=content.base_url,
                                                     penalty=1)

        # get all streams
        all_streams: BrokerStreamContentNodeCollection = {}
        for stream in self.streams.values():
            if stream.state == StreamState.STREAMING:
                c_nodes: List[BrokerStreamContentNode] = []
                for content, viewers in stream.contents.items():
                    new_node = BrokerStreamContentNode(_stream_id=stream.stream_id,
                                                       node_id=content.id,
                                                       max_viewers=viewers.max_clients,
                                                       current_viewers=viewers.current_clients)
                    c_nodes.append(new_node)
                all_streams[stream.stream_id] = c_nodes

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


class EncoderNoStreamSlotAvailable(Exception):
    pass


class CouldNotContactBrokerError(Exception):
    pass


class BrokerNotFound(Exception):
    pass