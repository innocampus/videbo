from aiohttp.web import Request
from aiohttp.web import HTTPSeeOther
from aiohttp.web import HTTPForbidden
from aiohttp.web import HTTPNotFound
from aiohttp.web import HTTPServiceUnavailable
from aiohttp.web import HTTPOk
from aiohttp.web import RouteTableDef
from os.path import splitext as path_splitext
from livestreaming.auth import BaseJWTData
from livestreaming.auth import ensure_jwt_data_and_role
from livestreaming.auth import Role
from livestreaming.web import register_route_with_cors
from livestreaming.web import ensure_json_body
from livestreaming.broker.grid import BrokerGrid
from livestreaming.broker.api.models import BrokerGridModel
from livestreaming.broker.api.models import BrokerRedirectJWTData
from livestreaming.broker import broker_settings

routes = RouteTableDef()
grid = BrokerGrid()


@register_route_with_cors(routes, "GET", r"/api/broker/redirect/{stream_id:\d+}/{playlist:[a-z0-9]+}.m3u8")
@ensure_jwt_data_and_role(Role.client)
async def redirect(request: Request, data: BrokerRedirectJWTData):
    """

    :param request:
    :param data:
    :raises HTTPForbidden
    :raises HTTPNotFound
    :raises HTTPServiceUnavailable
    :raises HTTPSeeOther
    """
    playlist = request.match_info["playlist"]
    stream_id = int(request.match_info['stream_id'])

    if data.stream_id != stream_id:
        raise HTTPForbidden()

    content_nodes = grid.get_stream(data.stream_id)
    if content_nodes is None:
        raise HTTPNotFound()
    available_nodes = list(filter(grid.is_available, content_nodes))
    if len(available_nodes) == 0:
        grid.add_to_wait_queue(stream_id)
        raise HTTPServiceUnavailable(headers={"Retry-After": broker_settings.http_retry_after})
    best_node = available_nodes.pop()
    if not len(available_nodes) == 0:
        least_penalty = grid.get_penalty_ratio(base_url=best_node)
        for node in available_nodes:
            penalty = grid.get_penalty_ratio(base_url=node)
            if penalty < least_penalty:
                best_node = node
                least_penalty = penalty
    url = f"{best_node}/api/content/playlist/{stream_id}/{playlist}.m3u8?{request.query_string}"
    grid.increment_clients(base_url=best_node)
    raise HTTPSeeOther(location=url)


@routes.get("/api/broker/state")
@ensure_jwt_data_and_role(Role.manager)
async def fetch_state(_: Request, __: BaseJWTData):
    """
    :param _: Mandatory for routing
    :param __: Mandatory for ensure_jwt_data_and_role()
    :raises HTTPOk
    """
    raise HTTPOk(body=grid.json_model())


@routes.post("/api/broker/streams")
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def update_grid(_: Request, __: BaseJWTData, model: BrokerGridModel):
    """
    Updates grid via web request.
    :param _: Mandatory for routing
    :param __: Mandatory for ensure_jwt_data_and_role()
    :param model:
    :raises HTTPOk
    """
    grid.update(model)
    raise HTTPOk()
