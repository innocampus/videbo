from aiohttp.web import Request
from aiohttp.web import HTTPSeeOther
from aiohttp.web import HTTPForbidden
from aiohttp.web import HTTPNotFound
from aiohttp.web import HTTPServiceUnavailable
from aiohttp.web import HTTPOk
from aiohttp.web import RouteTableDef
from aiohttp.client_exceptions import ClientConnectorError
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


@register_route_with_cors(routes, "GET", "/api/broker/redirect/{playlist}")
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
    playlist = request.match_info.get("playlist")
    stream_id = int(path_splitext(playlist)[0])
    if data.stream_id != stream_id:
        raise HTTPForbidden()
    content_nodes = grid.get_stream(data.stream_id)
    if content_nodes is None:
        raise HTTPNotFound()
    available_nodes = list(filter(grid.is_available, content_nodes))
    if len(available_nodes) == 0:
        raise HTTPServiceUnavailable(headers={"Retry-After": broker_settings.http_retry_after})
    best_node = available_nodes.pop()
    if not len(available_nodes) == 0:
        least_penalty = grid.get_penalty_ratio(host=best_node)
        for node in available_nodes:
            penalty = grid.get_penalty_ratio(host=node)
            if penalty < least_penalty:
                best_node = node
                least_penalty = penalty
    url = f"https://{best_node}/{playlist}?{request.query_string}"
    grid.increment_clients(host=best_node)
    raise HTTPSeeOther(location=url)


@routes.get("/api/broker/state")
@ensure_jwt_data_and_role(Role.manager)
async def fetch_state(request: Request, not_used: BaseJWTData):
    """
    :param request:
    :param not_used:
    :raises HTTPOk
    """
    model = {
        "streams": grid.get_streams(),
        "content_nodes": grid.get_content_nodes()
    }
    raise HTTPOk(body=f"{model}")


@routes.post("/api/broker/streams")
@ensure_jwt_data_and_role(Role.manager)
@ensure_json_body()
async def update_grid(request: Request, not_used: BaseJWTData, model: BrokerGridModel):
    """
    Updates grid via web request.
    :param request:
    :param model:
    :raises HTTPOk
    """
    grid.update(model)
    raise HTTPOk()
