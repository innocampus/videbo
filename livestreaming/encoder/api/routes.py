from aiohttp.web import Request, RouteTableDef
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body
from livestreaming.encoder import encoder_settings
from livestreaming.encoder.streams import stream_collection, StreamIdAlreadyExistsError, EncoderStream
from .models import NewStreamCreated, NewStreamReturn, NewStreamParams, EncoderStreamStatus, EncoderStatus

routes = RouteTableDef()


@routes.post(r'/api/encoder/stream/new/{stream_id:\d}')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def new_stream(request: Request, jwt_data: BaseJWTData, json: NewStreamParams):
    """Manager requests encoder to open a port and start a new stream to HLS encoding."""
    stream_id = int(request.match_info['stream_id'])
    try:
        stream = stream_collection.create_new_stream(stream_id, json.ip_range)
        stream.start()

        new_stream_data = NewStreamCreated(url=stream.get_public_url())
        return_data = NewStreamReturn(success=True, stream=new_stream_data)
        return json_response(return_data)
    except StreamIdAlreadyExistsError:
        return_data = NewStreamReturn(success=False, error="stream_id_already_exists")
        return json_response(return_data, status=409)


@routes.get(r'/api/encoder/state')
@ensure_jwt_data_and_role(Role.manager)
async def new_stream(request: Request, jwt_data: BaseJWTData):
    streams_status = []
    stream: EncoderStream
    for stream in stream_collection.streams.items():
        status = EncoderStreamStatus(stream_id=stream.stream_id, state=stream.state,
                                     state_last_update=stream.state_last_update)
        streams_status.append(status)

    ret = EncoderStatus(max_streams=encoder_settings.max_streams, current_streams=len(stream_collection.streams),
                        streams=streams_status)
    return json_response(ret)
