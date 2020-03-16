import asyncio
from time import time
from typing import Dict, Optional
from livestreaming.web import HTTPClient, HTTPResponseError
from livestreaming.auth import BaseJWTData, Role
from livestreaming.encoder.api.models import StreamState, NewStreamReturn
from livestreaming.content.api.models import StartStreamDistributionInfo
from . import logger


class Stream:
    def __init__(self, stream_id: int):
        self.stream_id: int = stream_id
        self.encoder_streamer_url: str = ''
        self.username: str = ''
        self.password: str = ''
        self.state: StreamState = StreamState.UNKNOWN
        self.state_last_update: float = 0

    async def tell_encoder(self):
        jwt_data = BaseJWTData.construct(role='manager')
        url = f'http://localhost:9010/api/encoder/stream/new/{self.stream_id}'

        try:
            ret: NewStreamReturn
            status, ret = await HTTPClient.internal_request('GET', url, jwt_data, None, NewStreamReturn)
            if ret.success:
                self.username = ret.stream.username
                self.password = ret.stream.password
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


class StreamCollection:
    def __init__(self):
        self.streams: Dict[int, Stream] = {}
        self.last_stream_id = 1

    def create_new_stream(self) -> Stream:
        self.last_stream_id += 1
        new_stream = Stream(self.last_stream_id)
        self.streams[new_stream.stream_id] = new_stream
        return new_stream

    def get_stream_by_id(self, stream_id: int) -> Stream:
        return self.streams[stream_id]


stream_collection = StreamCollection()


class EncoderCreateNewStreamError(Exception):
    pass


class ContentStartStreamingError(Exception):
    pass
