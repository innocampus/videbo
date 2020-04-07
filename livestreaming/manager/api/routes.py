import asyncio
from aiohttp.web import Request, RouteTableDef
from aiohttp.web_exceptions import HTTPNotFound
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body
from livestreaming.manager.streams import stream_collection
from .models import LMSNewStreamReturn, LMSNewStreamParams, AllStreamsStatus
from livestreaming.manager import logger

routes = RouteTableDef()


@routes.post(r'/api/manager/stream/new')
@ensure_jwt_data_and_role(Role.lms)
@ensure_json_body()
async def new_stream(request: Request, jwt_data: BaseJWTData, json: LMSNewStreamParams):
    """LMS requests manager to set up a new stream."""
    try:
        stream = stream_collection.create_new_stream(json.ip_range, json.rtmps, json.lms_stream_instance_id)
        await stream.encoder_listening_started

        await stream.tell_content()
        await stream_collection.tell_broker()

        new_stream_data = stream.get_status(True)
        return_data = LMSNewStreamReturn(success=True, stream=new_stream_data)
        return json_response(return_data)
    except Exception as e:
        error_type = type(e).__name__
        return_data = LMSNewStreamReturn(success=False, error=error_type)
        logger.exception("Exception in new_stream")
        return json_response(return_data, status=500)


@routes.get(r'/api/manager/stream/{stream_id:\d}/status')
@ensure_jwt_data_and_role(Role.lms)
async def get_stream_status(request: Request, _jwt_data: BaseJWTData):
    try:
        stream_id = int(request.match_info['stream_id'])
        stream = stream_collection.get_stream_by_id(stream_id)
        return json_response(stream.get_status(True))
    except KeyError:
        raise HTTPNotFound()


@routes.get(r'/api/manager/streams')
@ensure_jwt_data_and_role(Role.lms)
async def get_all_streams_status(request: Request, _jwt_data: BaseJWTData):
    try:
        streams_list = []
        for _, stream in stream_collection.streams.items():
            streams_list.append(stream.get_status(False))

        status = AllStreamsStatus(streams=streams_list)
        return json_response(status)
    except KeyError:
        raise HTTPNotFound()

