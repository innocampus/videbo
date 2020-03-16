from aiohttp.web import Request, Response, RouteTableDef
from aiohttp.web_exceptions import HTTPNotAcceptable
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import json_response, ensure_json_body
from livestreaming.content.streams_fetcher import stream_fetcher_collection, StreamFetcher, AlreadyFetchingStreamError
from livestreaming.content import logger
from .models import StartStreamDistributionInfo

routes = RouteTableDef()


@routes.get(r'/api/content/playlist/{stream_id:\d}.m3u8')
#@ensure_jwt_data_and_role(Role.client)
async def get_playlist(request: Request):
    """Manager requests encoder to open a port and start a new stream to HLS encoding."""
    stream_id = int(request.match_info['stream_id'])
    # TODO check if id equals id given in JWT

    stream = stream_fetcher_collection.get_fetcher_by_id(stream_id)
    return Response(body=stream.current_playlist, content_type='application/x-mpegURL')


@routes.post(r'/api/content/stream/start/{stream_id:\d}')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def start_stream(request: Request, jwt_data: BaseJWTData, data: StartStreamDistributionInfo):
    try:
        new_fetcher = StreamFetcher(data.stream_id, data.encoder_base_url)
        stream_fetcher_collection.start_fetching_stream(new_fetcher)
        return Response()

    except AlreadyFetchingStreamError:
        logger.info(f"Manager requested to start a new streaming, but stream with id {data.stream_id} is already streamed")
        raise HTTPNotAcceptable()
