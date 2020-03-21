import asyncio
from aiohttp.web import Request, RouteTableDef
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body
from livestreaming.manager.streams import stream_collection
from .models import LMSNewStreamCreated, LMSNewStreamReturn, LMSNewStreamParams
from livestreaming.manager import logger

routes = RouteTableDef()


@routes.post(r'/api/manager/stream/new')
@ensure_jwt_data_and_role(Role.lms)
@ensure_json_body()
async def new_stream(request: Request, jwt_data: BaseJWTData, json: LMSNewStreamParams):
    """LMS requests manager to set up a new stream."""
    try:
        stream = stream_collection.create_new_stream(json.ip_range)
        await stream.tell_encoder()
        await asyncio.sleep(30)
        await stream.tell_content()
        await stream_collection.tell_broker()

        new_stream_data = LMSNewStreamCreated(stream_id=stream.stream_id, streamer_url=stream.encoder_streamer_url,
                                              viewer_broker_url='', ip_restricted=(json.ip_range is not None))
        return_data = LMSNewStreamReturn(success=True, stream=new_stream_data)
        return json_response(return_data)
    except Exception as e:
        return_data = LMSNewStreamReturn(success=False, error="some error")
        file = e.__traceback__.tb_frame.f_code.co_filename
        logger.error(f"error raised at {file} on line {e.__traceback__.tb_lineno}")
        raise e
        # return json_response(return_data, status=500)
