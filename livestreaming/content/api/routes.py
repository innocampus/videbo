from asyncio import get_event_loop
from aiohttp.web import Request, Response, RouteTableDef, FileResponse
from aiohttp.web_exceptions import HTTPNotAcceptable, HTTPSeeOther, HTTPNotFound, HTTPForbidden, HTTPOk
from pathlib import Path
from livestreaming import settings
from livestreaming.auth import Role, BaseJWTData, ensure_jwt_data_and_role
from livestreaming.web import ensure_json_body, register_route_with_cors, json_response
from livestreaming.broker.api.models import BrokerRedirectJWTData
from livestreaming.content.streams_fetcher import stream_fetcher_collection, StreamFetcher, AlreadyFetchingStreamError
from livestreaming.content import content_logger, content_settings
from livestreaming.content.clients import client_collection
from .models import StartStreamDistributionInfo, ContentStatus

routes = RouteTableDef()


@register_route_with_cors(routes, "GET", r"/api/content/playlist/{stream_id:\d+}/{playlist:[a-z0-9]+}.m3u8")
@ensure_jwt_data_and_role(Role.client)
async def get_playlist(request: Request, jwt_token: BrokerRedirectJWTData):
    """Client asks for a playlist."""
    stream_id = int(request.match_info['stream_id'])
    if stream_id != jwt_token.stream_id or 'jwt' not in request.query:
        raise HTTPForbidden()

    jwt = request.query['jwt']

    # TEST CODE BEGIN
    import random
    if random.randint(0, 10) == 2:
        raise HTTPSeeOther(location=stream_fetcher_collection.get_broker_url(stream_id, jwt))
    # TEST CODE END

    try:
        stream_fetcher = stream_fetcher_collection.get_fetcher_by_id(stream_id)

        if not client_collection.serve_client(stream_id, jwt_token.rid):
            raise HTTPSeeOther(location=stream_fetcher_collection.get_broker_url(stream_id, jwt))

        playlist = request.match_info['playlist']
        if playlist == 'main':
            content = stream_fetcher.get_main_playlist(jwt)
        else:
            # sub playlists are numbered
            sub_no = int(playlist)
            content = stream_fetcher.get_sub_playlist(sub_no)

        return Response(body=content, content_type='application/x-mpegURL')
    except KeyError:
        raise HTTPSeeOther(location=stream_fetcher_collection.get_broker_url(stream_id, jwt))


@register_route_with_cors(routes, "GET", r"/data/hls/{stream_id:\d+}/{file:[a-z0-9_]+\.ts}")
async def get_segment(request: Request):
    """Serving segments for development purposes."""
    if not settings.general.dev_mode:
        raise HTTPNotFound()

    stream_id = int(request.match_info['stream_id'])
    file = request.match_info['file']
    path = Path(content_settings.hls_temp_dir, str(stream_id), file)
    if not (await get_event_loop().run_in_executor(None, path.is_file)):
        content_logger.error('File does not exist: %s', path)
        return HTTPNotFound()

    return FileResponse(path)


@routes.post(r'/api/content/stream/start/{stream_id:\d+}')
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def start_stream(_request: Request, _jwt_data: BaseJWTData, data: StartStreamDistributionInfo):
    try:
        new_fetcher = StreamFetcher(data.stream_id, data.encoder_base_url)
        stream_fetcher_collection.start_fetching_stream(new_fetcher)
        stream_fetcher_collection.broker_base_url = data.broker_base_url

        client_collection.add_stream(data.stream_id)

        return Response()

    except AlreadyFetchingStreamError:
        content_logger.info(f"Manager requested to start a new streaming, but stream with id {data.stream_id} is already streamed")
        raise HTTPNotAcceptable()


@routes.get(r'/api/content/stream/{stream_id:\d+}/destroy')
@ensure_jwt_data_and_role(Role.manager)
async def destroy_stream(request: Request, __: BaseJWTData):
    try:
        stream_id = int(request.match_info['stream_id'])
    except ValueError:
        raise HTTPNotAcceptable()
    client_collection.remove_stream(stream_id)
    stream_fetcher_collection.get_fetcher_by_id(stream_id).destroy()
    raise HTTPOk()


@routes.get(r'/api/content/status')
@ensure_jwt_data_and_role(Role.manager)
async def get_status(_request: Request, _jwt_data: BaseJWTData):
    client_collection.purge()

    streams = {}
    for stream in stream_fetcher_collection.fetcher.values():
        streams[stream.stream_id] = 0  # TODO

    ret = ContentStatus(max_clients=100, current_clients=0, streams=streams)  # TODO
    return json_response(ret)
