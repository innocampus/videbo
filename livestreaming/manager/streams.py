from typing import Optional, Union
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.auth import BaseJWTData
from livestreaming.streams import Stream, StreamCollection
from livestreaming.encoder.api.models import NewStreamReturn, NewStreamParams
from livestreaming.content.api.models import StartStreamDistributionInfo
from livestreaming.broker.api.models import BrokerContentNode, BrokerGridModel, BrokerStreamCollection
from livestreaming.manager.api.models import StreamStatusFull, StreamStatus
from . import logger


class ManagerStream(Stream):
    def __init__(self, stream_id: int, ip_range: Optional[str]):
        super().__init__(stream_id, ip_range, logger)
        self.encoder_streamer_url: str = ''

    async def tell_encoder(self):
        jwt_data = BaseJWTData.construct(role='manager')
        url = f'http://localhost:9010/api/encoder/stream/new/{self.stream_id}'
        stream_params = NewStreamParams(ip_range=self.ip_range_str)

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request('POST', url, jwt_data, stream_params, NewStreamReturn)
            if ret.success:
                self.encoder_streamer_url = ret.stream.url
                logger.info(f"New stream created, encoder streamer url {self.encoder_streamer_url}")
            else:
                logger.error(f"Could not create a new stream: {ret.error}")
                raise EncoderCreateNewStreamError()
        except HTTPResponseError as error:
            logger.error(f"Could not create a new stream: {error}")
            raise EncoderCreateNewStreamError()

    async def tell_content(self):
        jwt_data = BaseJWTData.construct(role='manager')
        url = f'http://localhost:9020/api/content/stream/start/{self.stream_id}'
        encoder_url = f'http://localhost:9010'
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
            'lms_stream_instance_id': 0,
            'state': self.state,
            'state_last_update': self.state_last_update,
            'viewers': 0,  # TODO
            'thumbnail_urls': []  # TODO
        }

        if full:
            data['streamer_url'] = self.encoder_streamer_url
            data['streamer_key'] = '' # TODO
            data['streamer_ip_restricted'] = self.is_ip_restricted
            data['streamer_connection_time_left'] = 300
            data['viewer_broker_url'] = '' # TODO
            return StreamStatusFull(**data)
        else:
            return StreamStatus(**data)


class ManagerStreamCollection(StreamCollection[ManagerStream]):
    def __init__(self):
        self.last_stream_id = 1

    def create_new_stream(self, ip_range: Optional[str] = None) -> ManagerStream:
        self.last_stream_id += 1
        new_stream = ManagerStream(self.last_stream_id, ip_range)
        self.streams[new_stream.stream_id] = new_stream
        return new_stream

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


stream_collection = ManagerStreamCollection()


class EncoderCreateNewStreamError(Exception):
    pass


class ContentStartStreamingError(Exception):
    pass


class CouldNotContactBrokerError(Exception):
    pass
