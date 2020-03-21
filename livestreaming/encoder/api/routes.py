from aiohttp.web import Request, RouteTableDef
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body
from livestreaming.encoder.streams import stream_collection, StreamIdAlreadyExistsError
from .models import NewStreamCreated, NewStreamReturn, NewStreamParams

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

        new_stream_data = NewStreamCreated(url=stream.get_url())
        return_data = NewStreamReturn(success=True, stream=new_stream_data)
        return json_response(return_data)
    except StreamIdAlreadyExistsError:
        return_data = NewStreamReturn(success=False, error="stream_id_already_exists")
        return json_response(return_data, status=409)


