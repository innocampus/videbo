import asyncio
from aiohttp.web import Request, RouteTableDef
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response
from livestreaming.manager.streams import stream_collection
from .models import LMSNewStreamCreated, LMSNewStreamReturn

routes = RouteTableDef()


@routes.get(r'/api/manager/stream/new')
@ensure_jwt_data_and_role(Role.lms)
async def new_stream(request: Request, jwt_data: BaseJWTData):
    """LMS requests manager to set up a new stream."""
    try:
        stream = stream_collection.create_new_stream()
        await stream.tell_encoder()
        await asyncio.sleep(30)
        await stream.tell_content()
        await stream_collection.tell_broker()

        new_stream_data = LMSNewStreamCreated(stream_id=stream.stream_id, streamer_url=stream.encoder_streamer_url,
                                              streamer_username=stream.username, streamer_password=stream.password,
                                              viewer_broker_url='')
        return_data = LMSNewStreamReturn(success=True, stream=new_stream_data)
        return json_response(return_data)
    except:
        return_data = LMSNewStreamReturn(success=False, error="some_error")
        return json_response(return_data, status=500)
