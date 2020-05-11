import asyncio
from aiohttp.web import RouteTableDef
from aiohttp.web import Request
from aiohttp.web_exceptions import HTTPNotAcceptable
from aiohttp.web_exceptions import HTTPOk
from aiohttp.web_exceptions import HTTPInternalServerError
from aiohttp.web_exceptions import HTTPConflict
from videbo.auth import Role
from videbo.auth import BaseJWTData
from videbo.auth import ensure_jwt_data_and_role
from videbo.web import ensure_json_body
from videbo.web import json_response
from videbo.encoder import encoder_settings
from videbo.encoder.streams import stream_collection
from videbo.encoder.streams import StreamIdAlreadyExistsError
from videbo.encoder.streams import EncoderStream
from videbo.streams import StreamState
from .models import NewStreamParams
from .models import EncoderStatus
from .models import EncoderStreamStatus
from .models import NewStreamReturn
from .models import NewStreamCreated
from .models import StreamRecordingStartParams
from .models import StreamRecordingStopParams
from .models import StreamRecordingMeta

routes = RouteTableDef()


@routes.post(r'/api/encoder/stream/new/{stream_id:\d+}')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def new_stream(request: Request, _jwt_data: BaseJWTData, json: NewStreamParams):
    """Manager requests encoder to open a port and start a new stream to HLS encoding."""
    stream_id = int(request.match_info['stream_id'])
    try:
        stream = stream_collection.create_new_stream(stream_id, json.ip_range, json.rtmps)
        stream.start()

        await asyncio.wait([stream.waiting_for_connection_event.wait(), stream.control_task],
                           return_when=asyncio.FIRST_COMPLETED)

        if stream.state < StreamState.ERROR:
            new_stream_data = NewStreamCreated(rtmp_port=stream.public_port,
                                               rtmp_stream_key=stream.rtmp_stream_key,
                                               encoder_subdir_name=stream.encoder_subdir_name)
            return_data = NewStreamReturn(success=True, stream=new_stream_data)
            return json_response(return_data)
        else:
            return json_response(NewStreamReturn(success=False, error=f"unknown_error ({stream.state})"),
                                 status=HTTPInternalServerError.status_code)

    except StreamIdAlreadyExistsError:
        return_data = NewStreamReturn(success=False, error="stream_id_already_exists")
        return json_response(return_data, status=HTTPConflict.status_code)


@routes.get(r'/api/encoder/status')
@ensure_jwt_data_and_role(Role.manager)
async def encoder_status(_request: Request, _jwt_data: BaseJWTData):
    streams_status = {}
    stream: EncoderStream
    for _, stream in stream_collection.streams.items():
        status = EncoderStreamStatus(stream_id=stream.stream_id, state=stream.state,
                                     state_last_update=stream.state_last_update)
        streams_status[stream.stream_id] = status

    ret = EncoderStatus(max_streams=encoder_settings.max_streams, current_streams=len(stream_collection.streams),
                        streams=streams_status)
    return json_response(ret)


@routes.get(r'/api/encoder/stream/{stream_id:\d+}/destroy')
@ensure_jwt_data_and_role(Role.manager)
async def destroy_stream(request: Request, __: BaseJWTData):
    try:
        stream_id = int(request.match_info['stream_id'])
        stream = stream_collection.get_stream_by_id(stream_id)
    except ValueError:
        raise HTTPNotAcceptable()
    except KeyError:
        raise HTTPOk()

    stream.destroy()
    raise HTTPOk()


@routes.post(r'/api/encoder/stream/{stream_id:\d+}/recording/start')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def recording_start(_request: Request, _jwt_data: BaseJWTData, _json: StreamRecordingStartParams):
    raise NotImplementedError()


@routes.post(r'/api/encoder/stream/{stream_id:\d+}/recording/stop')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def recording_stop(_request: Request, _jwt_data: BaseJWTData, _json: StreamRecordingStopParams):
    raise NotImplementedError()


@routes.get(r'/api/encoder/stream/{stream_id:\d+}/recording/status')
@ensure_jwt_data_and_role(Role.manager)
async def recordings_status(_request: Request, _jwt_data: BaseJWTData):
    raise NotImplementedError()  # return a StreamRecordingsStatusReturn


@routes.patch(r'/api/encoder/stream/{stream_id:\d+}/recording/{recording_id:\d+}')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def recording_edit(_request: Request, _jwt_data: BaseJWTData, _json: StreamRecordingMeta):
    """Update the matadata of a recording."""
    raise NotImplementedError()
